defmodule Mezzanine.Memory.InvalidationCoordinator do
  @moduledoc """
  Coordinates governed memory invalidation decisions.

  Durable storage, access-graph mutation, and cache consumers are owner
  callbacks. This module owns the fail-closed contract: reason validation,
  parent-chain cascade planning, cluster invalidation message shape, and
  hash-verifiable invalidation proof emission.
  """

  alias Mezzanine.Audit.MemoryProofToken
  alias Mezzanine.ConfigRegistry.ClusterInvalidation

  @reasons [
    :user_deletion,
    :source_correction,
    :source_deletion,
    :policy_change,
    :tenant_offboarding,
    :operator_suppression,
    :semantic_quarantine,
    :retention_expiry
  ]
  @access_revocation_reasons [:user_deletion, :tenant_offboarding]
  @required_string_fields [
    :tenant_ref,
    :root_fragment_id,
    :trace_id,
    :invalidate_policy_ref
  ]
  @ordering_fields [:source_node_ref, :commit_lsn, :commit_hlc]
  @normalizable_keys @required_string_fields ++
                       @ordering_fields ++
                       [
                         :access_projection_hash,
                         :applied_policies,
                         :authority_ref,
                         :effective_at,
                         :effective_at_epoch,
                         :evidence_refs,
                         :fragment_id,
                         :installation_ref,
                         :kind,
                         :parent_fragment_id,
                         :policy_id,
                         :reason,
                         :reason_string,
                         :tenant_ref,
                         :tier,
                         :trace_id,
                         :version
                       ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @type callback_opts :: keyword()
  @type invalidation_result :: %{
          invalidations: [map()],
          cluster_invalidations: [ClusterInvalidation.t()],
          access_revocation: term(),
          proof_token: MemoryProofToken.t()
        }

  @spec invalidate(map() | keyword(), callback_opts()) ::
          {:ok, invalidation_result()} | {:error, term()}
  def invalidate(request, opts \\ [])

  def invalidate(request, opts) when (is_map(request) or is_list(request)) and is_list(opts) do
    with {:ok, context} <- normalize_context(request),
         {:ok, fragments} <- call(opts, :list_fragment_family, [context]),
         {:ok, invalidations} <- build_invalidations(context, fragments),
         {:ok, inserted_invalidations} <- insert_invalidations(invalidations, context, opts),
         {:ok, policy_messages} <- maybe_publish_policy_invalidation(context, opts),
         {:ok, memory_messages} <-
           publish_memory_invalidations(inserted_invalidations, context, opts),
         {:ok, access_revocation} <- maybe_revoke_access_edges(context, opts),
         :ok <- ok_callback(call(opts, :invalidate_caches, [inserted_invalidations, context])),
         proof_token <- proof_token(context, inserted_invalidations, access_revocation),
         {:ok, proof_token} <- call(opts, :emit_proof, [proof_token, context]) do
      {:ok,
       %{
         invalidations: inserted_invalidations,
         cluster_invalidations: policy_messages ++ memory_messages,
         access_revocation: access_revocation,
         proof_token: proof_token
       }}
    end
  end

  def invalidate(_request, _opts), do: {:error, :invalid_invalidation_request}

  @spec reasons() :: [atom()]
  def reasons, do: @reasons

  defp normalize_context(request) do
    attrs = normalize_attrs(request)

    with {:ok, reason} <- normalize_reason(fetch(attrs, :reason)),
         :ok <- require_ordering(attrs),
         :ok <- require_strings(attrs, @required_string_fields),
         :ok <- require_positive_integer(attrs, :effective_at_epoch),
         :ok <- require_datetime(attrs, :effective_at),
         :ok <- require_non_empty_list(attrs, :evidence_refs),
         :ok <- require_map(attrs, :authority_ref) do
      {:ok,
       attrs
       |> Map.put(:reason, reason)
       |> Map.put(:reason_string, Atom.to_string(reason))
       |> Map.put(:root_fragment_id, fetch(attrs, :root_fragment_id))
       |> Map.put(:tenant_ref, fetch(attrs, :tenant_ref))
       |> Map.put(:trace_id, fetch(attrs, :trace_id))
       |> Map.put(:source_node_ref, fetch(attrs, :source_node_ref))
       |> Map.put(:commit_lsn, fetch(attrs, :commit_lsn))
       |> Map.put(:commit_hlc, fetch(attrs, :commit_hlc))
       |> Map.put(:effective_at, fetch(attrs, :effective_at))
       |> Map.put(:effective_at_epoch, fetch(attrs, :effective_at_epoch))
       |> Map.put(:evidence_refs, fetch(attrs, :evidence_refs))
       |> Map.put(:authority_ref, fetch(attrs, :authority_ref))
       |> Map.put(:invalidate_policy_ref, fetch(attrs, :invalidate_policy_ref))}
    end
  end

  defp build_invalidations(context, fragments) when is_list(fragments) do
    fragments = Enum.map(fragments, &normalize_attrs/1)
    by_id = Map.new(fragments, &{string_value(&1, :fragment_id), &1})

    case Map.fetch(by_id, context.root_fragment_id) do
      {:ok, _root} ->
        invalidations =
          fragments
          |> Enum.flat_map(&invalidation_for_fragment(&1, by_id, context))
          |> Enum.sort_by(&{length(&1.parent_chain), &1.fragment_id})

        {:ok, invalidations}

      :error ->
        {:error, {:root_fragment_not_found, context.root_fragment_id}}
    end
  end

  defp build_invalidations(_context, other),
    do: {:error, {:invalid_fragment_family, other}}

  defp invalidation_for_fragment(fragment, by_id, context) do
    fragment_id = string_value(fragment, :fragment_id)

    cond do
      is_nil(fragment_id) ->
        []

      fragment_id == context.root_fragment_id ->
        [invalidation_row(fragment, [], context)]

      true ->
        case parent_chain(fragment, by_id, context.root_fragment_id) do
          {:ok, chain} -> [invalidation_row(fragment, chain, context)]
          :unrelated -> []
        end
    end
  end

  defp parent_chain(fragment, by_id, root_fragment_id) do
    fragment
    |> string_value(:parent_fragment_id)
    |> walk_parent_chain(by_id, root_fragment_id, [], [])
  end

  defp walk_parent_chain(nil, _by_id, _root_fragment_id, _seen, _chain), do: :unrelated

  defp walk_parent_chain(parent_id, by_id, root_fragment_id, seen, chain) do
    cond do
      parent_id in seen ->
        :unrelated

      parent_id == root_fragment_id ->
        {:ok, [parent_id | chain]}

      parent = Map.get(by_id, parent_id) ->
        parent
        |> string_value(:parent_fragment_id)
        |> walk_parent_chain(by_id, root_fragment_id, [parent_id | seen], [parent_id | chain])

      true ->
        :unrelated
    end
  end

  defp invalidation_row(fragment, parent_chain, context) do
    fragment_id = string_value(fragment, :fragment_id)
    invalidation_id = invalidation_id(context, fragment_id)

    %{
      invalidation_id: invalidation_id,
      tenant_ref: context.tenant_ref,
      installation_ref: fetch(context, :installation_ref),
      fragment_id: fragment_id,
      tier: normalize_tier(fetch(fragment, :tier)),
      reason: context.reason_string,
      effective_at: context.effective_at,
      effective_at_epoch: context.effective_at_epoch,
      source_node_ref: context.source_node_ref,
      commit_lsn: context.commit_lsn,
      commit_hlc: context.commit_hlc,
      invalidate_policy_ref: context.invalidate_policy_ref,
      authority_ref: context.authority_ref,
      evidence_refs: context.evidence_refs,
      parent_chain: parent_chain,
      root_fragment_id: context.root_fragment_id,
      access_projection_hash: string_value(fragment, :access_projection_hash),
      applied_policies: list_value(fragment, :applied_policies),
      metadata: %{
        "root_fragment_id" => context.root_fragment_id,
        "parent_chain" => parent_chain,
        "cascade_depth" => length(parent_chain)
      }
    }
  end

  defp insert_invalidations(invalidations, context, opts) do
    Enum.reduce_while(invalidations, {:ok, []}, fn invalidation, {:ok, acc} ->
      case call(opts, :insert_invalidation, [invalidation, context]) do
        {:ok, inserted} -> {:cont, {:ok, [inserted | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      error -> error
    end
  end

  defp maybe_publish_policy_invalidation(
         %{reason: :policy_change, policy_ref: policy_ref} = context,
         opts
       )
       when is_map(policy_ref) do
    message =
      ClusterInvalidation.new!(%{
        invalidation_id:
          "policy-invalidation://#{policy_ref_value(policy_ref, :policy_id)}/#{policy_ref_value(policy_ref, :version)}",
        tenant_ref: context.tenant_ref,
        topic:
          ClusterInvalidation.policy_topic!(
            tenant_ref: context.tenant_ref,
            installation_ref:
              policy_ref_value(policy_ref, :installation_ref) || fetch(context, :installation_ref),
            kind: policy_ref_value(policy_ref, :kind) || :invalidate,
            policy_id: policy_ref_value(policy_ref, :policy_id),
            version: policy_ref_value(policy_ref, :version)
          ),
        source_node_ref: context.source_node_ref,
        commit_lsn: context.commit_lsn,
        commit_hlc: context.commit_hlc,
        published_at: context.effective_at,
        metadata: %{
          "reason" => context.reason_string,
          "policy_id" => policy_ref_value(policy_ref, :policy_id),
          "policy_version" => policy_ref_value(policy_ref, :version),
          "policy_kind" => Atom.to_string(policy_ref_value(policy_ref, :kind) || :invalidate),
          "effective_at" =>
            DateTime.to_iso8601(
              policy_ref_value(policy_ref, :effective_at) || context.effective_at
            )
        }
      })

    with :ok <- publish_message(message, context, opts) do
      {:ok, [message]}
    end
  end

  defp maybe_publish_policy_invalidation(_context, _opts), do: {:ok, []}

  defp publish_memory_invalidations(invalidations, context, opts) do
    invalidations
    |> Enum.flat_map(&memory_messages(&1, context))
    |> Enum.reduce_while({:ok, []}, fn message, {:ok, acc} ->
      case publish_message(message, context, opts) do
        :ok -> {:cont, {:ok, [message | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, messages} -> {:ok, Enum.reverse(messages)}
      error -> error
    end
  end

  defp memory_messages(invalidation, context) do
    [
      build_memory_message(
        invalidation,
        context,
        ClusterInvalidation.fragment_topic!(context.tenant_ref, invalidation.fragment_id)
      ),
      build_memory_message(
        invalidation,
        context,
        ClusterInvalidation.invalidation_topic!(context.tenant_ref, invalidation.invalidation_id)
      )
    ]
  end

  defp build_memory_message(invalidation, context, topic) do
    ClusterInvalidation.new!(%{
      invalidation_id: invalidation.invalidation_id,
      tenant_ref: context.tenant_ref,
      topic: topic,
      source_node_ref: context.source_node_ref,
      commit_lsn: context.commit_lsn,
      commit_hlc: context.commit_hlc,
      published_at: context.effective_at,
      metadata: %{
        "tenant_ref" => context.tenant_ref,
        "root_fragment_id" => context.root_fragment_id,
        "fragment_id" => invalidation.fragment_id,
        "tier" => invalidation.tier,
        "reason" => context.reason_string,
        "effective_at_epoch" => context.effective_at_epoch,
        "parent_chain" => invalidation.parent_chain,
        "invalidation_id" => invalidation.invalidation_id,
        "source_node_ref" => context.source_node_ref,
        "commit_lsn" => context.commit_lsn,
        "commit_hlc" => context.commit_hlc
      }
    })
  end

  defp publish_message(message, context, opts),
    do: ok_callback(call(opts, :publish_cluster_invalidation, [message, context]))

  defp maybe_revoke_access_edges(%{reason: reason} = context, opts)
       when reason in @access_revocation_reasons do
    call(opts, :revoke_access_edges, [context])
  end

  defp maybe_revoke_access_edges(_context, _opts), do: {:ok, nil}

  defp proof_token(context, invalidations, access_revocation) do
    MemoryProofToken.new!(%{
      proof_hash_version: "m7a.v1",
      proof_id:
        "invalidation-proof://#{hash_segment(context.tenant_ref <> "|" <> context.root_fragment_id <> "|" <> context.reason_string <> "|" <> Integer.to_string(context.effective_at_epoch))}",
      kind: :invalidate,
      tenant_ref: context.tenant_ref,
      installation_id: fetch(context, :installation_ref),
      t_event: context.effective_at,
      epoch_used: context.effective_at_epoch,
      source_node_ref: context.source_node_ref,
      commit_lsn: context.commit_lsn,
      commit_hlc: context.commit_hlc,
      policy_refs: proof_policy_refs(context),
      fragment_ids: Enum.map(invalidations, & &1.fragment_id),
      transform_hashes: [],
      access_projection_hashes:
        invalidations
        |> Enum.map(& &1.access_projection_hash)
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq(),
      trace_id: context.trace_id,
      parent_fragment_id: context.root_fragment_id,
      child_fragment_id: nil,
      evidence_refs: context.evidence_refs,
      governance_decision_ref: context.authority_ref,
      metadata: %{
        invalidation_reason: context.reason_string,
        invalidation_ids: Enum.map(invalidations, & &1.invalidation_id),
        root_fragment_id: context.root_fragment_id,
        parent_chains: Map.new(invalidations, &{&1.fragment_id, &1.parent_chain}),
        access_revocation: access_revocation
      }
    })
  end

  defp proof_policy_refs(%{policy_ref: policy_ref}) when is_map(policy_ref) do
    [
      %{
        id: policy_ref_value(policy_ref, :policy_id),
        version: policy_ref_value(policy_ref, :version)
      }
    ]
  end

  defp proof_policy_refs(context), do: [%{id: context.invalidate_policy_ref, version: 1}]

  defp require_ordering(attrs) do
    case Enum.find(@ordering_fields, &missing_ordering_field?(attrs, &1)) do
      nil -> :ok
      field -> {:error, {:missing_ordering_evidence, field}}
    end
  end

  defp missing_ordering_field?(attrs, :commit_hlc), do: not is_map(fetch(attrs, :commit_hlc))

  defp missing_ordering_field?(attrs, field) do
    case fetch(attrs, field) do
      value when is_binary(value) -> String.trim(value) == ""
      _other -> true
    end
  end

  defp require_strings(attrs, fields) do
    missing =
      Enum.reject(fields, fn field ->
        case fetch(attrs, field) do
          value when is_binary(value) -> String.trim(value) != ""
          _other -> false
        end
      end)

    if missing == [], do: :ok, else: {:error, {:missing_required_fields, missing}}
  end

  defp require_positive_integer(attrs, field) do
    case fetch(attrs, field) do
      value when is_integer(value) and value > 0 -> :ok
      _other -> {:error, {:invalid_positive_integer, field}}
    end
  end

  defp require_datetime(attrs, field) do
    case fetch(attrs, field) do
      %DateTime{} -> :ok
      _other -> {:error, {:invalid_datetime, field}}
    end
  end

  defp require_non_empty_list(attrs, field) do
    case fetch(attrs, field) do
      [_head | _tail] -> :ok
      _other -> {:error, {:missing_required_fields, [field]}}
    end
  end

  defp require_map(attrs, field) do
    case fetch(attrs, field) do
      value when is_map(value) and map_size(value) > 0 -> :ok
      _other -> {:error, {:missing_required_fields, [field]}}
    end
  end

  defp normalize_reason(reason) when reason in @reasons, do: {:ok, reason}

  defp normalize_reason(reason) when is_binary(reason) do
    case Enum.find(@reasons, &(Atom.to_string(&1) == reason)) do
      nil -> {:error, {:unsupported_invalidation_reason, reason}}
      reason -> {:ok, reason}
    end
  end

  defp normalize_reason(reason), do: {:error, {:unsupported_invalidation_reason, reason}}

  defp normalize_tier(tier) when is_atom(tier), do: Atom.to_string(tier)
  defp normalize_tier(tier) when is_binary(tier), do: tier
  defp normalize_tier(_tier), do: nil

  defp invalidation_id(context, fragment_id) do
    seed =
      [
        context.tenant_ref,
        context.root_fragment_id,
        fragment_id,
        context.reason_string,
        Integer.to_string(context.effective_at_epoch),
        context.source_node_ref,
        context.commit_lsn
      ]
      |> Enum.join("|")

    "memory-invalidation://#{hash_segment(seed)}"
  end

  defp hash_segment(value) do
    value
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, 32)
  end

  defp call(opts, callback, args) do
    case Keyword.get(opts, callback) do
      fun when is_function(fun, length(args)) ->
        apply(fun, args)

      nil ->
        {:error, {:missing_callback, callback}}

      _other ->
        {:error, {:invalid_callback, callback}}
    end
  end

  defp ok_callback(:ok), do: :ok
  defp ok_callback({:ok, _result}), do: :ok
  defp ok_callback({:error, reason}), do: {:error, reason}
  defp ok_callback(other), do: {:error, {:invalid_callback_result, other}}

  defp normalize_attrs(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_attrs()

  defp normalize_attrs(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    Map.get(@key_lookup, key, key)
  end

  defp fetch(map, key) when is_map(map),
    do: Map.get(map, key) || Map.get(map, Atom.to_string(key))

  defp string_value(source, key) do
    case fetch(source, key) do
      value when is_binary(value) and value != "" -> value
      _other -> nil
    end
  end

  defp list_value(source, key) do
    case fetch(source, key) do
      values when is_list(values) -> values
      nil -> []
      value -> [value]
    end
  end

  defp policy_ref_value(policy_ref, key), do: fetch(policy_ref, key)
end
