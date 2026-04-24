defmodule Mezzanine.Memory.SharedScopeRegistry do
  @moduledoc """
  Epoch-stable registry for scopes that may receive shared memory.

  The registry fails closed by default. Rows are durable, carry node/order
  evidence, and publish through the M7A cluster invalidation plane.
  """

  import Ecto.Query

  alias Mezzanine.ConfigRegistry.{ClusterInvalidation, Repo}
  alias Mezzanine.Memory.SharedScopeRegistryEntry

  @cache_table :mezzanine_shared_scope_registry_cache
  @global_tenant_ref "tenant://global"

  @spec scope_registered?(String.t(), pos_integer(), keyword()) :: boolean()
  def scope_registered?(scope_ref, epoch, opts \\ []) do
    tenant_ref = tenant_ref(opts)

    with {:ok, scope_ref} <- non_empty_string(scope_ref, :scope_ref),
         {:ok, epoch} <- positive_integer(epoch, :epoch) do
      cache_fetch({tenant_ref, scope_ref, epoch}, fn ->
        Repo.exists?(registered_query(tenant_ref, scope_ref, epoch))
      end)
    else
      _error -> false
    end
  end

  @spec register(String.t(), map(), keyword()) :: :ok | {:error, term()}
  def register(scope_ref, governance_ref, opts \\ [])

  def register(scope_ref, governance_ref, opts) when is_map(governance_ref) do
    with {:ok, attrs} <- register_attrs(scope_ref, governance_ref, opts),
         {:ok, _entry} <- insert_registration(attrs) do
      :ok
    end
  end

  def register(_scope_ref, _governance_ref, _opts), do: {:error, :invalid_governance_ref}

  @spec deregister(String.t(), map(), keyword()) :: :ok | {:error, term()}
  def deregister(scope_ref, governance_ref, opts \\ [])

  def deregister(scope_ref, governance_ref, opts) when is_map(governance_ref) do
    with {:ok, attrs} <- deregister_attrs(scope_ref, governance_ref, opts),
         {:ok, _count} <- update_deregistration(attrs) do
      :ok
    end
  end

  def deregister(_scope_ref, _governance_ref, _opts), do: {:error, :invalid_governance_ref}

  @spec invalidation_topic!(String.t(), String.t()) :: String.t()
  def invalidation_topic!(tenant_ref, scope_ref) do
    topic!([
      "memory",
      "shared_scope",
      ClusterInvalidation.hash_segment(required_string!(tenant_ref, :tenant_ref)),
      ClusterInvalidation.hash_segment(required_string!(scope_ref, :scope_ref))
    ])
  end

  defp insert_registration(attrs) do
    Repo.transaction(fn ->
      entry =
        %SharedScopeRegistryEntry{}
        |> SharedScopeRegistryEntry.changeset(attrs)
        |> Repo.insert!()

      :ok = publish_invalidation!(entry, :register, attrs.activation_epoch, attrs.governance_ref)
      clear_cache()
      entry
    end)
  rescue
    error in Ecto.InvalidChangesetError -> {:error, error.changeset}
  else
    {:ok, entry} -> {:ok, entry}
    {:error, reason} -> {:error, reason}
  end

  defp update_deregistration(attrs) do
    case Repo.transaction(fn ->
           {count, entries} = deregister_entries(attrs)
           publish_deregistrations!(entries, attrs)
           clear_cache()
           count
         end) do
      {:ok, count} -> {:ok, count}
      {:error, reason} -> {:error, reason}
    end
  end

  defp deregister_entries(attrs) do
    SharedScopeRegistryEntry
    |> where([entry], entry.tenant_ref == ^attrs.tenant_ref)
    |> where([entry], entry.scope_ref == ^attrs.scope_ref)
    |> where([entry], entry.activation_epoch < ^attrs.deregistration_epoch)
    |> where(
      [entry],
      is_nil(entry.deregistration_epoch) or
        entry.deregistration_epoch >= ^attrs.deregistration_epoch
    )
    |> select([entry], entry)
    |> Repo.update_all(
      set: [
        deregistration_epoch: attrs.deregistration_epoch,
        deregistration_governance_ref: attrs.governance_ref,
        updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      ]
    )
  end

  defp publish_deregistrations!(entries, attrs) do
    Enum.each(entries, fn entry ->
      entry = %{
        entry
        | deregistration_epoch: attrs.deregistration_epoch,
          source_node_ref: attrs.source_node_ref,
          commit_lsn: attrs.commit_lsn,
          commit_hlc: attrs.commit_hlc
      }

      :ok =
        publish_invalidation!(
          entry,
          :deregister,
          attrs.deregistration_epoch,
          attrs.governance_ref
        )
    end)
  end

  defp register_attrs(scope_ref, governance_ref, opts) do
    with {:ok, scope_ref} <- non_empty_string(scope_ref, :scope_ref),
         {:ok, activation_epoch} <-
           positive_integer(Keyword.get(opts, :activation_epoch), :activation_epoch),
         {:ok, source_node_ref} <-
           non_empty_string(Keyword.get(opts, :source_node_ref), :source_node_ref),
         {:ok, commit_hlc} <- commit_hlc(Keyword.get(opts, :commit_hlc)) do
      tenant_ref = tenant_ref(opts)

      {:ok,
       %{
         tenant_ref: tenant_ref,
         scope_ref: scope_ref,
         governance_ref: governance_ref,
         activation_epoch: activation_epoch,
         source_node_ref: source_node_ref,
         commit_lsn: Keyword.get(opts, :commit_lsn) || current_wal_lsn!(),
         commit_hlc: commit_hlc,
         invalidation_topic: invalidation_topic!(tenant_ref, scope_ref)
       }}
    end
  end

  defp deregister_attrs(scope_ref, governance_ref, opts) do
    with {:ok, scope_ref} <- non_empty_string(scope_ref, :scope_ref),
         {:ok, deregistration_epoch} <-
           positive_integer(Keyword.get(opts, :deregistration_epoch), :deregistration_epoch),
         {:ok, source_node_ref} <-
           non_empty_string(Keyword.get(opts, :source_node_ref), :source_node_ref),
         {:ok, commit_hlc} <- commit_hlc(Keyword.get(opts, :commit_hlc)) do
      tenant_ref = tenant_ref(opts)

      {:ok,
       %{
         tenant_ref: tenant_ref,
         scope_ref: scope_ref,
         governance_ref: governance_ref,
         deregistration_epoch: deregistration_epoch,
         source_node_ref: source_node_ref,
         commit_lsn: Keyword.get(opts, :commit_lsn) || current_wal_lsn!(),
         commit_hlc: commit_hlc
       }}
    end
  end

  defp registered_query(tenant_ref, scope_ref, epoch) do
    SharedScopeRegistryEntry
    |> where([entry], entry.tenant_ref == ^tenant_ref)
    |> where([entry], entry.scope_ref == ^scope_ref)
    |> where([entry], entry.activation_epoch <= ^epoch)
    |> where([entry], is_nil(entry.deregistration_epoch) or entry.deregistration_epoch > ^epoch)
  end

  defp publish_invalidation!(entry, action, epoch, governance_ref) do
    message =
      ClusterInvalidation.new!(%{
        invalidation_id: "shared-scope-registry://#{action}/#{entry.id}/#{epoch}",
        tenant_ref: entry.tenant_ref,
        topic: entry.invalidation_topic,
        source_node_ref: entry.source_node_ref,
        commit_lsn: entry.commit_lsn,
        commit_hlc: entry.commit_hlc,
        published_at: DateTime.utc_now() |> DateTime.truncate(:microsecond),
        metadata: %{
          "registry_action" => Atom.to_string(action),
          "scope_ref" => entry.scope_ref,
          "activation_epoch" => entry.activation_epoch,
          "deregistration_epoch" => entry.deregistration_epoch,
          "effective_epoch" => epoch,
          "governance_ref" => governance_ref
        }
      })

    case ClusterInvalidation.publish(message) do
      :ok -> :ok
      {:error, reason} -> Repo.rollback({:shared_scope_invalidation_failed, reason})
    end
  end

  defp cache_fetch(key, fun) do
    ensure_cache!()

    case :ets.lookup(@cache_table, key) do
      [{^key, value}] ->
        value

      [] ->
        value = fun.()
        :ets.insert(@cache_table, {key, value})
        value
    end
  end

  defp clear_cache do
    ensure_cache!()
    :ets.delete_all_objects(@cache_table)
    :ok
  end

  defp ensure_cache! do
    case :ets.whereis(@cache_table) do
      :undefined ->
        :ets.new(@cache_table, [:named_table, :public, read_concurrency: true])

      _tid ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp tenant_ref(opts), do: Keyword.get(opts, :tenant_ref, @global_tenant_ref)

  defp commit_hlc(value) when is_map(value), do: {:ok, normalize_hlc(value)}
  defp commit_hlc(_value), do: {:error, {:missing_ordering_evidence, :commit_hlc}}

  defp normalize_hlc(value) do
    %{
      "w" => Map.get(value, "w") || Map.fetch!(value, :wall_ns),
      "l" => Map.get(value, "l") || Map.fetch!(value, :logical),
      "n" => Map.get(value, "n") || Map.fetch!(value, :node)
    }
  rescue
    KeyError -> value
  end

  defp positive_integer(value, _key) when is_integer(value) and value > 0, do: {:ok, value}
  defp positive_integer(_value, key), do: {:error, {:invalid_positive_integer, key}}

  defp non_empty_string(value, _key) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: {:error, :empty_string}, else: {:ok, value}
  end

  defp non_empty_string(_value, key), do: {:error, {:missing_field, key}}

  defp required_string!(value, key) do
    case non_empty_string(value, key) do
      {:ok, value} ->
        value

      {:error, reason} ->
        raise ArgumentError, "#{key} must be a non-empty string: #{inspect(reason)}"
    end
  end

  defp topic!(segments), do: Enum.map_join(segments, ".", &required_string!(&1, :topic_segment))

  defp current_wal_lsn! do
    %{rows: [[commit_lsn]]} = Repo.query!("SELECT pg_current_wal_lsn()::text", [])
    commit_lsn
  end
end
