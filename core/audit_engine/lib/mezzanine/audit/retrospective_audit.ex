defmodule Mezzanine.Audit.RetrospectiveAudit do
  @moduledoc """
  Deterministic replay engine for governed-memory proof tokens.
  """

  alias Mezzanine.Audit.{MemoryProofToken, MemoryProofTokenStore}

  @m7a_ordering_fields [:source_node_ref, :commit_lsn, :commit_hlc]

  @type mode :: :verify_as_of_recall | :re_evaluate_under_current | :drift_report
  @type result :: {:ok, map()} | {:error, term()}

  @spec verify_as_of_recall(String.t(), keyword()) :: result()
  def verify_as_of_recall(proof_id, opts \\ []) when is_binary(proof_id) and is_list(opts) do
    with {:ok, token} <- fetch_verified_token(proof_id, opts),
         {:ok, evaluation} <- evaluate_historical(token, opts),
         :ok <- validate_policy_refs(token, opts) do
      token
      |> base_report(:verify_as_of_recall)
      |> Map.merge(evaluation_report(evaluation))
      |> Map.put(:status, :verified)
      |> emit_artifact(opts)
    end
  end

  @spec re_evaluate_under_current(String.t(), keyword()) :: result()
  def re_evaluate_under_current(proof_id, opts \\ [])
      when is_binary(proof_id) and is_list(opts) do
    with {:ok, token} <- fetch_verified_token(proof_id, opts),
         {:ok, evaluation} <- evaluate_current(token, token.fragment_ids, opts),
         {:ok, current_policy_refs} <- current_policy_refs(token, opts) do
      token
      |> base_report(:re_evaluate_under_current)
      |> Map.merge(evaluation_report(evaluation))
      |> Map.put(:current_epoch, evaluation.epoch)
      |> Map.put(:current_policy_refs, current_policy_refs)
      |> emit_artifact(opts)
    end
  end

  @spec drift_report(String.t(), keyword()) :: result()
  def drift_report(proof_id, opts \\ []) when is_binary(proof_id) and is_list(opts) do
    with {:ok, token} <- fetch_verified_token(proof_id, opts),
         {:ok, historical} <- evaluate_historical(token, opts),
         {:ok, candidate_ids} <- current_candidate_fragment_ids(token, opts),
         {:ok, current} <- evaluate_current(token, candidate_ids, opts),
         {:ok, current_policy_refs} <- current_policy_refs(token, opts) do
      token
      |> base_report(:drift_report)
      |> Map.merge(%{
        current_epoch: current.epoch,
        historical_admitted_fragment_ids: historical.admitted_fragment_ids,
        current_admitted_fragment_ids: current.admitted_fragment_ids,
        newly_inadmissible_fragment_ids:
          list_difference(historical.admitted_fragment_ids, current.admitted_fragment_ids),
        newly_admissible_fragment_ids:
          list_difference(current.admitted_fragment_ids, historical.admitted_fragment_ids),
        transform_changes: transform_changes(historical.fragments, current.fragments),
        policy_version_changes: policy_version_changes(token.policy_refs, current_policy_refs),
        historical_inadmissible_fragments: historical.inadmissible_fragments,
        current_inadmissible_fragments: current.inadmissible_fragments
      })
      |> emit_artifact(opts)
    end
  end

  defp fetch_verified_token(proof_id, opts) do
    proof_store = Keyword.get(opts, :proof_store, MemoryProofTokenStore)

    with {:ok, raw_token} <- proof_store.fetch(proof_id),
         {:ok, token} <- normalize_token(raw_token),
         :ok <- validate_ordering_evidence(token),
         :ok <- validate_snapshot_epoch(token),
         :ok <- MemoryProofToken.verify_hash(token) do
      {:ok, token}
    end
  end

  defp normalize_token(%MemoryProofToken{} = token), do: {:ok, token}
  defp normalize_token(attrs) when is_map(attrs), do: MemoryProofToken.new(attrs)

  defp validate_ordering_evidence(%MemoryProofToken{proof_hash_version: "m7a.v1"} = token) do
    missing =
      Enum.filter(@m7a_ordering_fields, fn field ->
        missing_value?(Map.get(token, field))
      end)

    case missing do
      [] -> :ok
      fields -> {:error, {:missing_proof_token_fields, fields}}
    end
  end

  defp validate_ordering_evidence(%MemoryProofToken{}), do: :ok

  defp validate_snapshot_epoch(%MemoryProofToken{} = token) do
    token.metadata
    |> metadata_value(:snapshot_epoch)
    |> case do
      nil ->
        :ok

      snapshot_epoch when snapshot_epoch == token.epoch_used ->
        :ok

      snapshot_epoch ->
        {:error,
         {:snapshot_epoch_mismatch,
          %{epoch_used: token.epoch_used, snapshot_epoch: snapshot_epoch}}}
    end
  end

  defp evaluate_historical(%MemoryProofToken{} = token, opts) do
    with {:ok, evaluation} <- evaluate_epoch(token, token.fragment_ids, token.epoch_used, opts),
         :ok <- reject_missing_fragments(evaluation) do
      {:ok, evaluation}
    end
  end

  defp evaluate_current(%MemoryProofToken{} = token, fragment_ids, opts) do
    epoch = current_epoch(token, opts)
    evaluate_epoch(token, fragment_ids, epoch, opts)
  end

  defp evaluate_epoch(%MemoryProofToken{} = token, fragment_ids, epoch, opts) do
    with {:ok, fragments} <- fetch_fragments(token, fragment_ids, epoch, opts) do
      fragment_map = Map.new(fragments, &{fragment_value(&1, :fragment_id), &1})
      missing_ids = Enum.reject(fragment_ids, &Map.has_key?(fragment_map, &1))

      results =
        fragment_ids
        |> Enum.filter(&Map.has_key?(fragment_map, &1))
        |> Enum.map(fn fragment_id ->
          evaluate_fragment(token, Map.fetch!(fragment_map, fragment_id), epoch, opts)
        end)

      {:ok,
       %{
         epoch: epoch,
         fragments: Enum.map(fragment_ids, &Map.get(fragment_map, &1)) |> Enum.reject(&is_nil/1),
         missing_fragment_ids: missing_ids,
         admitted_fragment_ids: admitted_fragment_ids(results),
         inadmissible_fragments:
           missing_inadmissible(missing_ids) ++ inadmissible_fragments(results),
         fragment_results: results
       }}
    end
  end

  defp evaluate_fragment(%MemoryProofToken{} = token, fragment, epoch, opts) do
    checks = [
      access_projection_check(token, fragment),
      parent_chain_check(token, fragment, epoch, opts),
      accessibility_check(token, fragment, epoch, opts)
    ]

    reasons = checks |> Enum.reject(&(&1 == :ok)) |> Enum.map(fn {:error, reason} -> reason end)
    fragment_id = fragment_value(fragment, :fragment_id)

    %{
      fragment_id: fragment_id,
      admitted?: reasons == [],
      reasons: reasons,
      fragment: fragment
    }
  end

  defp access_projection_check(%MemoryProofToken{} = token, fragment) do
    fragment_id = fragment_value(fragment, :fragment_id)
    expected_hash = expected_access_projection_hash(token, fragment_id)
    actual_hash = fragment_value(fragment, :access_projection_hash)

    cond do
      is_nil(expected_hash) -> :ok
      expected_hash == actual_hash -> :ok
      true -> {:error, :access_projection_hash_mismatch}
    end
  end

  defp parent_chain_check(%MemoryProofToken{} = token, fragment, epoch, opts) do
    fragment_store = Keyword.fetch!(opts, :fragment_store)
    fragment_id = fragment_value(fragment, :fragment_id)

    with {:ok, actual_chain} <- parent_chain(fragment_store, token.tenant_ref, fragment_id, epoch) do
      case recorded_parent_chain(token, fragment_id) do
        nil -> :ok
        ^actual_chain -> :ok
        _recorded_chain -> {:error, :source_lineage_parent_chain_mismatch}
      end
    end
  end

  defp accessibility_check(%MemoryProofToken{} = token, fragment, epoch, opts) do
    access_graph_store = Keyword.fetch!(opts, :access_graph_store)
    tuple = effective_access_tuple(fragment)

    access_graph_store
    |> replay_views(token.tenant_ref, token.user_ref, token.agent_ref, epoch, tuple)
    |> case do
      %{graph_admissible?: true} -> :ok
      %{"graph_admissible?" => true} -> :ok
      _views -> {:error, :accessibility_predicate_failed}
    end
  end

  defp fetch_fragments(%MemoryProofToken{} = token, fragment_ids, epoch, opts) do
    fragment_store = Keyword.fetch!(opts, :fragment_store)
    result = fragment_store.fetch_fragments(token.tenant_ref, fragment_ids, snapshot_epoch: epoch)

    case result do
      {:ok, fragments} when is_list(fragments) -> {:ok, fragments}
      fragments when is_list(fragments) -> {:ok, fragments}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_fragment_store_result, other}}
    end
  end

  defp reject_missing_fragments(%{missing_fragment_ids: []}), do: :ok

  defp reject_missing_fragments(%{missing_fragment_ids: missing}),
    do: {:error, {:missing_fragments, missing}}

  defp validate_policy_refs(%MemoryProofToken{} = token, opts) do
    policy_registry = Keyword.fetch!(opts, :policy_registry)

    case policy_registry.validate_refs_at(token.policy_refs, policy_context(token),
           at: token.t_event,
           snapshot_epoch: token.epoch_used
         ) do
      {:ok, _policies} -> :ok
      {:error, reason} -> {:error, reason}
      other -> {:error, {:invalid_policy_registry_result, other}}
    end
  end

  defp current_policy_refs(%MemoryProofToken{} = token, opts) do
    policy_registry = Keyword.fetch!(opts, :policy_registry)

    if function_exported?(policy_registry, :current_policy_refs, 2) do
      case policy_registry.current_policy_refs(policy_context(token), opts) do
        {:ok, refs} when is_list(refs) -> {:ok, refs}
        refs when is_list(refs) -> {:ok, refs}
        {:error, reason} -> {:error, reason}
        other -> {:error, {:invalid_current_policy_refs_result, other}}
      end
    else
      {:ok, token.policy_refs}
    end
  end

  defp current_candidate_fragment_ids(%MemoryProofToken{} = token, opts) do
    fragment_store = Keyword.fetch!(opts, :fragment_store)

    if function_exported?(fragment_store, :current_candidate_fragment_ids, 2) do
      case fragment_store.current_candidate_fragment_ids(token, opts) do
        {:ok, ids} when is_list(ids) -> {:ok, ids}
        ids when is_list(ids) -> {:ok, ids}
        {:error, reason} -> {:error, reason}
        other -> {:error, {:invalid_current_candidate_result, other}}
      end
    else
      {:ok, token.fragment_ids}
    end
  end

  defp current_epoch(%MemoryProofToken{} = token, opts) do
    fragment_store = Keyword.fetch!(opts, :fragment_store)

    if function_exported?(fragment_store, :current_epoch, 1) do
      fragment_store.current_epoch(token.tenant_ref)
    else
      token.epoch_used
    end
  end

  defp replay_views(access_graph_store, tenant_ref, user_ref, agent_ref, epoch, tuple) do
    cond do
      function_exported?(access_graph_store, :replay_views, 5) ->
        access_graph_store.replay_views(tenant_ref, user_ref, agent_ref, epoch, tuple)

      function_exported?(access_graph_store, :graph_admissible?, 5) ->
        %{
          graph_admissible?:
            access_graph_store.graph_admissible?(tenant_ref, tuple, user_ref, agent_ref, epoch)
        }
    end
  end

  defp parent_chain(fragment_store, tenant_ref, fragment_id, epoch) do
    cond do
      function_exported?(fragment_store, :parent_chain, 3) ->
        normalize_parent_chain_result(
          fragment_store.parent_chain(tenant_ref, fragment_id, snapshot_epoch: epoch)
        )

      function_exported?(fragment_store, :source_lineage_parent_chain, 3) ->
        normalize_parent_chain_result(
          fragment_store.source_lineage_parent_chain(tenant_ref, fragment_id,
            snapshot_epoch: epoch
          )
        )

      true ->
        {:ok, []}
    end
  end

  defp normalize_parent_chain_result({:ok, chain}) when is_list(chain), do: {:ok, chain}
  defp normalize_parent_chain_result(chain) when is_list(chain), do: {:ok, chain}
  defp normalize_parent_chain_result({:error, reason}), do: {:error, reason}
  defp normalize_parent_chain_result(other), do: {:error, {:invalid_parent_chain_result, other}}

  defp expected_access_projection_hash(%MemoryProofToken{} = token, fragment_id) do
    token.fragment_ids
    |> Enum.zip(token.access_projection_hashes)
    |> Enum.find_value(fn
      {^fragment_id, hash} -> hash
      _other -> nil
    end)
  end

  defp effective_access_tuple(fragment) do
    %{
      fragment_id: fragment_value(fragment, :fragment_id),
      access_agents: list_value(fragment, :access_agents),
      access_resources: list_value(fragment, :access_resources),
      access_scopes: list_value(fragment, :access_scopes)
    }
  end

  defp evaluation_report(evaluation) do
    %{
      snapshot_epoch: evaluation.epoch,
      admitted_fragment_ids: evaluation.admitted_fragment_ids,
      inadmissible_fragments: evaluation.inadmissible_fragments,
      missing_fragment_ids: evaluation.missing_fragment_ids,
      fragment_refs:
        Enum.map(evaluation.fragments, fn fragment ->
          %{
            fragment_id: fragment_value(fragment, :fragment_id),
            source_node_ref: fragment_value(fragment, :source_node_ref),
            parent_fragment_id: fragment_value(fragment, :parent_fragment_id)
          }
        end),
      access_projection_refs:
        Enum.map(evaluation.fragments, fn fragment ->
          %{
            fragment_id: fragment_value(fragment, :fragment_id),
            access_projection_hash: fragment_value(fragment, :access_projection_hash)
          }
        end)
    }
  end

  defp base_report(%MemoryProofToken{} = token, mode) do
    %{
      mode: mode,
      proof_id: token.proof_id,
      proof_hash: token.proof_hash,
      proof_hash_version: token.proof_hash_version,
      tenant_ref: token.tenant_ref,
      installation_id: token.installation_id,
      subject_id: token.subject_id,
      execution_id: token.execution_id,
      user_ref: token.user_ref,
      agent_ref: token.agent_ref,
      trace_id: token.trace_id,
      policy_refs: token.policy_refs,
      graph_epoch_refs: [%{tenant_ref: token.tenant_ref, epoch: token.epoch_used}],
      source_proof_token: token,
      source_proof_token_refs: [
        %{
          proof_id: token.proof_id,
          proof_hash: token.proof_hash,
          source_node_ref: token.source_node_ref,
          commit_lsn: token.commit_lsn,
          commit_hlc: token.commit_hlc
        }
      ]
    }
  end

  defp emit_artifact(report, opts) do
    artifact_store = Keyword.get(opts, :artifact_store, __MODULE__.ArtifactStore)
    signature = signature(report)
    signed_report = Map.put(report, :audit_artifact, %{signature: signature})
    artifact_payload = Map.put(signed_report, :signature, signature)

    case artifact_store.emit(artifact_payload, opts) do
      {:ok, artifact_ref} ->
        {:ok,
         Map.put(signed_report, :audit_artifact, Map.merge(%{signature: signature}, artifact_ref))}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp admitted_fragment_ids(results) do
    results
    |> Enum.filter(& &1.admitted?)
    |> Enum.map(& &1.fragment_id)
  end

  defp inadmissible_fragments(results) do
    results
    |> Enum.reject(& &1.admitted?)
    |> Enum.map(fn result ->
      %{
        fragment_id: result.fragment_id,
        reason: List.first(result.reasons),
        reasons: result.reasons
      }
    end)
  end

  defp missing_inadmissible(missing_ids) do
    Enum.map(missing_ids, fn fragment_id ->
      %{fragment_id: fragment_id, reason: :missing_fragment, reasons: [:missing_fragment]}
    end)
  end

  defp transform_changes(historical_fragments, current_fragments) do
    current_by_id = Map.new(current_fragments, &{fragment_value(&1, :fragment_id), &1})

    historical_fragments
    |> Enum.filter(fn historical ->
      fragment_id = fragment_value(historical, :fragment_id)

      case Map.get(current_by_id, fragment_id) do
        nil ->
          false

        current ->
          fragment_value(historical, :transform_pipeline, []) !=
            fragment_value(current, :transform_pipeline, [])
      end
    end)
    |> Enum.map(fn historical ->
      fragment_id = fragment_value(historical, :fragment_id)
      current = Map.fetch!(current_by_id, fragment_id)

      %{
        fragment_id: fragment_id,
        historical: fragment_value(historical, :transform_pipeline, []),
        current: fragment_value(current, :transform_pipeline, [])
      }
    end)
  end

  defp policy_version_changes(historical_refs, current_refs) do
    if normalize_policy_refs(historical_refs) == normalize_policy_refs(current_refs) do
      []
    else
      [
        %{
          historical: historical_refs,
          current: current_refs
        }
      ]
    end
  end

  defp normalize_policy_refs(refs) do
    Enum.map(refs, &normalize_policy_ref/1)
  end

  defp normalize_policy_ref(ref) when is_map(ref) do
    %{
      id: metadata_value(ref, :id) || metadata_value(ref, :policy_id),
      version: metadata_value(ref, :version),
      kind: metadata_value(ref, :kind)
    }
  end

  defp list_difference(left, right) do
    right_set = MapSet.new(right)
    Enum.reject(left, &MapSet.member?(right_set, &1))
  end

  defp policy_context(%MemoryProofToken{} = token) do
    %{
      tenant_ref: token.tenant_ref,
      installation_ref: token.installation_id,
      user_ref: token.user_ref,
      agent_ref: token.agent_ref,
      trace_id: token.trace_id,
      snapshot_epoch: token.epoch_used
    }
  end

  defp recorded_parent_chain(%MemoryProofToken{} = token, fragment_id) do
    token.metadata
    |> metadata_value(:fragment_parent_chains)
    |> case do
      nil -> nil
      chains -> metadata_value(chains, fragment_id)
    end
  end

  defp fragment_value(fragment, key, default \\ nil)

  defp fragment_value(%_struct{} = fragment, key, default) do
    fragment
    |> Map.from_struct()
    |> fragment_value(key, default)
  end

  defp fragment_value(fragment, key, default) when is_map(fragment) do
    metadata_value(fragment, key) || default
  end

  defp list_value(fragment, key) do
    case fragment_value(fragment, key, []) do
      values when is_list(values) -> values
      nil -> []
      value -> [value]
    end
  end

  defp metadata_value(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp metadata_value(map, key) when is_map(map) and is_binary(key) do
    Map.get(map, key)
  end

  defp metadata_value(_map, _key), do: nil

  defp missing_value?(nil), do: true
  defp missing_value?(""), do: true
  defp missing_value?(value) when is_map(value), do: map_size(value) == 0
  defp missing_value?(_value), do: false

  defp signature(report) do
    "sha256:" <>
      (report
       |> canonical_json()
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower))
  end

  defp canonical_json(value), do: Jason.encode!(canonicalize(value))

  defp canonicalize(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp canonicalize(%MapSet{} = value), do: value |> MapSet.to_list() |> canonicalize()
  defp canonicalize(%_struct{} = value), do: value |> Map.from_struct() |> canonicalize()

  defp canonicalize(value) when is_map(value) do
    value
    |> Enum.map(fn {key, map_value} -> {to_string(key), canonicalize(map_value)} end)
    |> Enum.sort_by(&elem(&1, 0))
    |> Map.new()
  end

  defp canonicalize(value) when is_list(value), do: Enum.map(value, &canonicalize/1)
  defp canonicalize(value) when is_atom(value), do: Atom.to_string(value)
  defp canonicalize(value), do: value

  defmodule ArtifactStore do
    @moduledoc false

    alias Mezzanine.Audit.AuditAppend

    @spec emit(map(), keyword()) :: {:ok, map()} | {:error, term()}
    def emit(report, _opts) when is_map(report) do
      artifact_id = "retrospective-audit://" <> String.replace(report.signature, "sha256:", "")

      result =
        AuditAppend.append_fact(%{
          installation_id: report.installation_id || report.tenant_ref,
          subject_id: report.subject_id,
          execution_id: report.execution_id,
          evidence_id: artifact_id,
          trace_id: report.trace_id,
          causation_id: report.proof_id,
          fact_kind: :retrospective_audit,
          actor_ref: %{"component" => "Mezzanine.Audit.RetrospectiveAudit"},
          payload: json_safe(report),
          occurred_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
          idempotency_key: artifact_id
        })

      case result do
        {:ok, %{audit_fact_id: audit_fact_id}} ->
          {:ok, %{artifact_id: artifact_id, audit_fact_id: audit_fact_id, durable?: true}}

        {:error, reason} ->
          {:error, reason}
      end
    end

    defp json_safe(%DateTime{} = value), do: DateTime.to_iso8601(value)
    defp json_safe(%MapSet{} = value), do: value |> MapSet.to_list() |> json_safe()
    defp json_safe(%_struct{} = value), do: value |> Map.from_struct() |> json_safe()

    defp json_safe(value) when is_map(value) do
      Map.new(value, fn {key, map_value} -> {to_string(key), json_safe(map_value)} end)
    end

    defp json_safe(value) when is_list(value), do: Enum.map(value, &json_safe/1)
    defp json_safe(value) when is_atom(value), do: Atom.to_string(value)
    defp json_safe(value), do: value
  end
end
