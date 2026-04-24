defmodule Mezzanine.ConfigRegistry.PolicyRegistry do
  @moduledoc """
  Registry facade for Phase 7 governed-memory policies.
  """

  alias Mezzanine.ConfigRegistry
  alias Mezzanine.ConfigRegistry.{ClusterInvalidation, Policy, PolicyContracts, Repo}

  @type register_opt ::
          {:tenant_ref, String.t()}
          | {:installation_ref, String.t()}
          | {:effective_from, DateTime.t()}
          | {:effective_until, DateTime.t() | nil}
          | {:authoring_bundle_ref, map()}
          | {:trusted_registry_ref, map()}
          | {:source_node_ref, String.t()}
          | {:commit_hlc, map()}

  @spec register(struct(), [register_opt()]) :: {:ok, Policy.t()} | {:error, term()}
  def register(policy_contract, opts \\ []) when is_struct(policy_contract) and is_list(opts) do
    attrs = policy_attrs(policy_contract, opts)

    with :ok <- validate_invalidation_evidence(opts),
         :ok <- reject_conflict(attrs),
         {:ok, {policy, notifications}} <- register_with_invalidation(attrs, opts) do
      :ok = notify_policy_register(notifications)
      {:ok, policy}
    end
  end

  @spec resolve(atom() | String.t(), map(), keyword()) :: {:ok, Policy.t()} | {:error, :not_found}
  def resolve(kind, context, opts \\ []) when is_map(context) and is_list(opts) do
    kind = normalize_kind!(kind)
    at = opts[:at] || DateTime.utc_now()

    Policy
    |> Ash.read!(domain: ConfigRegistry)
    |> Enum.map(&normalize_policy/1)
    |> Enum.filter(&applicable?(&1, kind, context, at))
    |> Enum.sort_by(&resolution_rank/1, :desc)
    |> case do
      [policy | _policies] -> {:ok, policy}
      [] -> {:error, :not_found}
    end
  end

  @spec resolve_for_snapshot(atom() | String.t(), map(), term(), keyword()) ::
          {:ok, Policy.t()}
          | {:error,
             :not_found | {:invalid_snapshot_epoch, term()} | {:snapshot_epoch_mismatch, map()}}
  def resolve_for_snapshot(kind, context, snapshot_epoch, opts \\ [])
      when is_map(context) and is_list(opts) do
    with {:ok, snapshot_epoch} <- normalize_snapshot_epoch(snapshot_epoch),
         {:ok, context} <- bind_snapshot_context(context, snapshot_epoch),
         {:ok, opts} <- bind_snapshot_opts(opts, snapshot_epoch) do
      resolve(kind, context, opts)
    end
  end

  @spec validate_refs_at([map()], map(), keyword()) ::
          {:ok, [Policy.t()]}
          | {:error,
             {:missing_policy_ref, map()}
             | {:policy_ref_not_effective, map()}
             | {:invalid_policy_ref, term()}
             | {:invalid_snapshot_epoch, term()}}
  def validate_refs_at(policy_refs, context, opts \\ [])
      when is_list(policy_refs) and is_map(context) and is_list(opts) do
    at = Keyword.get(opts, :at, DateTime.utc_now())

    with :ok <- validate_optional_snapshot_epoch(opts) do
      validate_policy_refs(policy_refs, context, at)
    end
  end

  defp policy_attrs(policy_contract, opts) do
    %{
      policy_id: policy_contract.policy_id,
      tenant_ref: opts[:tenant_ref],
      installation_ref: opts[:installation_ref],
      kind: PolicyContracts.kind(policy_contract),
      version: policy_contract.version,
      granularity_scope: policy_contract.granularity_scope,
      spec: PolicyContracts.dump(policy_contract),
      effective_from:
        opts[:effective_from] || DateTime.utc_now() |> DateTime.truncate(:microsecond),
      effective_until: opts[:effective_until],
      authoring_bundle_ref: opts[:authoring_bundle_ref] || %{},
      trusted_registry_ref: opts[:trusted_registry_ref] || %{}
    }
  end

  defp validate_invalidation_evidence(opts) do
    preflight_attrs = %{
      invalidation_id: "policy-invalidation://preflight",
      tenant_ref: "tenant://preflight",
      topic: "memory.policy.preflight",
      source_node_ref: Keyword.get(opts, :source_node_ref),
      commit_lsn: Keyword.get(opts, :commit_lsn, "preflight"),
      commit_hlc: Keyword.get(opts, :commit_hlc),
      published_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
      metadata: %{}
    }

    case ClusterInvalidation.new(preflight_attrs) do
      {:ok, _message} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp register_with_invalidation(attrs, opts) do
    Repo.transaction(fn ->
      with {:ok, %Policy{} = policy, notifications} <-
             Policy.register(attrs, return_notifications?: true),
           :ok <- publish_policy_invalidation(policy, opts) do
        {normalize_policy(policy), notifications}
      else
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end

  defp notify_policy_register(notifications) do
    notifications
    |> List.wrap()
    |> case do
      [] ->
        :ok

      notifications ->
        Ash.Notifier.notify(notifications)
        :ok
    end
  end

  defp publish_policy_invalidation(%Policy{} = policy, opts) do
    source_node_ref = Keyword.get(opts, :source_node_ref)
    commit_hlc = Keyword.get(opts, :commit_hlc)

    message_attrs = %{
      invalidation_id: "policy-invalidation://#{policy.id}/#{policy.version}",
      tenant_ref: policy.tenant_ref || "tenant://global",
      topic:
        ClusterInvalidation.policy_topic!(
          tenant_ref: policy.tenant_ref,
          installation_ref: policy.installation_ref,
          kind: policy.kind,
          policy_id: policy.policy_id,
          version: policy.version
        ),
      source_node_ref: source_node_ref,
      commit_lsn: Keyword.get(opts, :commit_lsn) || current_wal_lsn!(),
      commit_hlc: commit_hlc,
      published_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
      metadata: %{
        "policy_id" => policy.policy_id,
        "kind" => Atom.to_string(policy.kind),
        "version" => policy.version,
        "policy_version" => policy.version,
        "effective_from" => DateTime.to_iso8601(policy.effective_from),
        "effective_until" => maybe_datetime(policy.effective_until),
        "granularity_scope" => Atom.to_string(policy.granularity_scope),
        "installation_ref" => policy.installation_ref
      }
    }

    case ClusterInvalidation.new(message_attrs) do
      {:ok, message} -> ClusterInvalidation.publish(message)
      {:error, reason} -> {:error, reason}
    end
  end

  defp current_wal_lsn! do
    %{rows: [[commit_lsn]]} = Repo.query!("SELECT pg_current_wal_lsn()::text", [])
    commit_lsn
  end

  defp maybe_datetime(nil), do: nil
  defp maybe_datetime(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp reject_conflict(attrs) do
    Policy
    |> Ash.read!(domain: ConfigRegistry)
    |> Enum.map(&normalize_policy/1)
    |> Enum.find(&conflicts?(&1, attrs))
    |> case do
      nil ->
        :ok

      %Policy{} ->
        {:error,
         {:conflicting_policy_precedence,
          %{
            policy_id: attrs.policy_id,
            kind: attrs.kind,
            version: attrs.version,
            granularity_scope: attrs.granularity_scope,
            tenant_ref: attrs.tenant_ref,
            installation_ref: attrs.installation_ref
          }}}
    end
  end

  defp conflicts?(%Policy{} = policy, attrs) do
    policy.policy_id == attrs.policy_id and
      policy.kind == attrs.kind and
      policy.version == attrs.version and
      policy.granularity_scope == attrs.granularity_scope and
      policy.tenant_ref == attrs.tenant_ref and
      policy.installation_ref == attrs.installation_ref and
      intervals_overlap?(
        policy.effective_from,
        policy.effective_until,
        attrs.effective_from,
        attrs.effective_until
      )
  end

  defp applicable?(%Policy{} = policy, kind, context, at) do
    policy.kind == kind and active_at?(policy, at) and scope_matches?(policy, context)
  end

  defp validate_ref_at(policy_ref, context, at) when is_map(policy_ref) do
    with {:ok, normalized_ref} <- normalize_policy_ref(policy_ref) do
      candidates =
        Policy
        |> Ash.read!(domain: ConfigRegistry)
        |> Enum.map(&normalize_policy/1)
        |> Enum.filter(&same_policy_ref?(&1, normalized_ref))

      cond do
        candidates == [] ->
          {:error, {:missing_policy_ref, public_policy_ref(normalized_ref)}}

        Enum.find(candidates, &(active_at?(&1, at) and scope_matches?(&1, context))) ->
          {:ok, Enum.find(candidates, &(active_at?(&1, at) and scope_matches?(&1, context)))}

        true ->
          {:error, {:policy_ref_not_effective, public_policy_ref(normalized_ref)}}
      end
    end
  end

  defp validate_ref_at(policy_ref, _context, _at), do: {:error, {:invalid_policy_ref, policy_ref}}

  defp validate_policy_refs(policy_refs, context, at) do
    policy_refs
    |> Enum.reduce_while([], &validate_policy_ref_reducer(&1, &2, context, at))
    |> finalize_policy_refs()
  end

  defp validate_policy_ref_reducer(policy_ref, policies, context, at) do
    case validate_ref_at(policy_ref, context, at) do
      {:ok, policy} -> {:cont, [policy | policies]}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp finalize_policy_refs({:error, reason}), do: {:error, reason}
  defp finalize_policy_refs(policies), do: {:ok, Enum.reverse(policies)}

  defp same_policy_ref?(%Policy{} = policy, ref) do
    policy.policy_id == ref.policy_id and policy.version == ref.version and
      (is_nil(ref.kind) or policy.kind == ref.kind)
  end

  defp normalize_policy_ref(policy_ref) do
    policy_id = fetch_context(policy_ref, :id) || fetch_context(policy_ref, :policy_id)
    version = fetch_context(policy_ref, :version)
    kind = fetch_context(policy_ref, :kind)

    cond do
      not is_binary(policy_id) or policy_id == "" ->
        {:error, {:invalid_policy_ref, policy_ref}}

      not is_integer(version) ->
        {:error, {:invalid_policy_ref, policy_ref}}

      true ->
        {:ok, %{policy_id: policy_id, version: version, kind: normalize_optional_kind(kind)}}
    end
  end

  defp normalize_optional_kind(nil), do: nil
  defp normalize_optional_kind(kind), do: normalize_kind!(kind)

  defp public_policy_ref(%{policy_id: policy_id, version: version}) do
    %{"id" => policy_id, "version" => version}
  end

  defp intervals_overlap?(left_from, left_until, right_from, right_until) do
    DateTime.compare(left_from, right_until || DateTime.from_unix!(4_102_444_800)) == :lt and
      DateTime.compare(right_from, left_until || DateTime.from_unix!(4_102_444_800)) == :lt
  end

  defp active_at?(%Policy{} = policy, at) do
    starts_before_or_at? = DateTime.compare(policy.effective_from, at) in [:lt, :eq]

    not_ended? =
      is_nil(policy.effective_until) or DateTime.compare(policy.effective_until, at) == :gt

    starts_before_or_at? and not_ended?
  end

  defp scope_matches?(%Policy{granularity_scope: :global, tenant_ref: nil}, _context), do: true

  defp scope_matches?(%Policy{granularity_scope: :tenant} = policy, context) do
    policy.tenant_ref == fetch_context(context, :tenant_ref)
  end

  defp scope_matches?(%Policy{granularity_scope: :installation} = policy, context) do
    policy.tenant_ref == fetch_context(context, :tenant_ref) and
      policy.installation_ref == fetch_context(context, :installation_ref)
  end

  defp scope_matches?(%Policy{granularity_scope: scope} = policy, context)
       when scope in [:workspace, :agent, :actor_role, :time_window] do
    policy.tenant_ref == fetch_context(context, :tenant_ref) and
      scoped_spec_matches?(policy.spec, scope, context)
  end

  defp scope_matches?(_policy, _context), do: false

  defp scoped_spec_matches?(spec, scope, context) do
    scope_key =
      %{
        workspace: :workspace_ref,
        agent: :agent_ref,
        actor_role: :actor_role_ref,
        time_window: :time_window_ref
      }
      |> Map.fetch!(scope)

    is_nil(fetch_spec(spec, scope_key)) or
      fetch_spec(spec, scope_key) == fetch_context(context, scope_key)
  end

  defp resolution_rank(%Policy{} = policy) do
    {
      PolicyContracts.precedence(policy.granularity_scope),
      policy.version,
      DateTime.to_unix(policy.effective_from, :microsecond)
    }
  end

  defp normalize_policy(%Policy{} = policy) do
    %{policy | spec: PolicyContracts.normalize_spec(policy.spec)}
  end

  defp normalize_snapshot_epoch(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp normalize_snapshot_epoch(value) do
    {:error, {:invalid_snapshot_epoch, value}}
  end

  defp validate_optional_snapshot_epoch(opts) do
    case Keyword.fetch(opts, :snapshot_epoch) do
      {:ok, snapshot_epoch} ->
        case normalize_snapshot_epoch(snapshot_epoch) do
          {:ok, _snapshot_epoch} -> :ok
          {:error, reason} -> {:error, reason}
        end

      :error ->
        :ok
    end
  end

  defp bind_snapshot_context(context, snapshot_epoch) do
    case fetch_context(context, :snapshot_epoch) do
      nil ->
        {:ok, Map.put(context, :snapshot_epoch, snapshot_epoch)}

      ^snapshot_epoch ->
        {:ok, Map.put(context, :snapshot_epoch, snapshot_epoch)}

      context_epoch ->
        {:error, {:snapshot_epoch_mismatch, %{bound: snapshot_epoch, context: context_epoch}}}
    end
  end

  defp bind_snapshot_opts(opts, snapshot_epoch) do
    case Keyword.fetch(opts, :snapshot_epoch) do
      {:ok, ^snapshot_epoch} ->
        {:ok, opts}

      {:ok, other_epoch} ->
        {:error, {:snapshot_epoch_mismatch, %{bound: snapshot_epoch, opts: other_epoch}}}

      :error ->
        {:ok, Keyword.put(opts, :snapshot_epoch, snapshot_epoch)}
    end
  end

  defp normalize_kind!(kind) do
    case PolicyContracts.policy_kind(kind) do
      {:ok, kind} -> kind
      {:error, reason} -> raise ArgumentError, "invalid policy kind: #{inspect(reason)}"
    end
  end

  defp fetch_context(context, key), do: Map.get(context, key) || Map.get(context, to_string(key))
  defp fetch_spec(spec, key), do: Map.get(spec, key) || Map.get(spec, to_string(key))
end
