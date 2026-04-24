defmodule Mezzanine.Audit.TemporalQueueRouting do
  @moduledoc """
  Audit-owned Temporal task queue routing helpers for Phase 7 memory workflows.

  Typed refs are represented in queue names by deterministic b32lower SHA-256
  segments. Audit tooling can persist the reverse lookup entries returned by
  this module without putting raw tenant or installation refs in Temporal queue
  names or worker identities.
  """

  @segment_size 20
  @typed_queue_regex ~r/\Amez\.(?:promotion|workflow_runtime)\.([a-z2-7]{20})\z/
  @node_shortname_regex ~r/\A[A-Za-z0-9_.-]+\z/

  @type reverse_lookup_entry :: %{
          hash_segment: String.t(),
          typed_ref: String.t(),
          ref_kind: atom(),
          queue: String.t()
        }

  alias Mezzanine.Audit.{Repo, TemporalQueueReverseLookupRecord}

  @doc "Returns the canonical b32lower SHA-256 segment for a typed ref."
  @spec hash_segment(String.t()) :: String.t()
  def hash_segment(typed_ref) when is_binary(typed_ref) do
    :sha256
    |> :crypto.hash(typed_ref)
    |> Base.encode32(case: :lower, padding: false)
    |> binary_part(0, @segment_size)
  end

  @doc "Returns the promotion task queue for an installation ref."
  @spec promotion_queue(String.t()) :: String.t()
  def promotion_queue(installation_ref) when is_binary(installation_ref) do
    "mez.promotion.#{hash_segment(installation_ref)}"
  end

  @doc "Returns the workflow runtime task queue for a tenant ref."
  @spec workflow_runtime_queue(String.t()) :: String.t()
  def workflow_runtime_queue(tenant_ref) when is_binary(tenant_ref) do
    "mez.workflow_runtime.#{hash_segment(tenant_ref)}"
  end

  @doc "Returns the canonical decision expiry queue."
  @spec decision_expiry_queue() :: String.t()
  def decision_expiry_queue, do: "mez.decision_expiry"

  @doc "Returns the canonical invalidation cascade queue."
  @spec invalidation_cascade_queue() :: String.t()
  def invalidation_cascade_queue, do: "mez.invalidation_cascade"

  @doc "Returns an audit reverse lookup entry for a hashed queue segment."
  @spec reverse_lookup_entry(:installation | :tenant, String.t()) :: reverse_lookup_entry()
  def reverse_lookup_entry(:installation, installation_ref) when is_binary(installation_ref) do
    %{
      hash_segment: hash_segment(installation_ref),
      typed_ref: installation_ref,
      ref_kind: :installation,
      queue: promotion_queue(installation_ref)
    }
  end

  def reverse_lookup_entry(:tenant, tenant_ref) when is_binary(tenant_ref) do
    %{
      hash_segment: hash_segment(tenant_ref),
      typed_ref: tenant_ref,
      ref_kind: :tenant,
      queue: workflow_runtime_queue(tenant_ref)
    }
  end

  @doc "Persists a reverse lookup row for a typed-ref task queue segment."
  @spec upsert_reverse_lookup(:installation | :tenant, String.t()) ::
          {:ok, TemporalQueueReverseLookupRecord.t()} | {:error, Ecto.Changeset.t()}
  def upsert_reverse_lookup(ref_kind, typed_ref) do
    attrs =
      ref_kind
      |> reverse_lookup_entry(typed_ref)
      |> Map.update!(:ref_kind, &Atom.to_string/1)

    %TemporalQueueReverseLookupRecord{}
    |> TemporalQueueReverseLookupRecord.changeset(attrs)
    |> Repo.insert(
      on_conflict: {:replace, [:typed_ref, :ref_kind, :queue, :updated_at]},
      conflict_target: :hash_segment,
      returning: true
    )
  end

  @doc "Fetches a reverse lookup row by hash segment."
  @spec fetch_reverse_lookup(String.t()) ::
          {:ok, TemporalQueueReverseLookupRecord.t()} | {:error, :not_found}
  def fetch_reverse_lookup(hash_segment) when is_binary(hash_segment) do
    case Repo.get_by(TemporalQueueReverseLookupRecord, hash_segment: hash_segment) do
      %TemporalQueueReverseLookupRecord{} = record -> {:ok, record}
      nil -> {:error, :not_found}
    end
  end

  @doc "Returns a Temporal worker identity that excludes PIDs and raw typed refs."
  @spec worker_identity(keyword()) :: String.t() | {:error, atom()}
  def worker_identity(opts) when is_list(opts) do
    with {:ok, node_shortname} <- fetch_valid_node_shortname(opts),
         {:ok, node_instance_id} <- fetch_valid_node_instance_id(opts),
         {:ok, worker_role} <- fetch_valid_worker_role(opts),
         {:ok, task_queue_hash} <- fetch_task_queue_hash(opts) do
      [
        node_shortname,
        binary_part(node_instance_id, 0, 8),
        worker_role,
        task_queue_hash
      ]
      |> Enum.join("/")
      |> ensure_identity_size()
    end
  end

  defp fetch_valid_node_shortname(opts) do
    case Keyword.get(opts, :node_shortname) do
      value when is_binary(value) and value != "" ->
        if Regex.match?(@node_shortname_regex, value) do
          {:ok, value}
        else
          {:error, :invalid_node_shortname}
        end

      _other ->
        {:error, :invalid_node_shortname}
    end
  end

  defp fetch_valid_node_instance_id(opts) do
    case Keyword.get(opts, :node_instance_id) do
      value when is_binary(value) and byte_size(value) >= 8 -> {:ok, value}
      _other -> {:error, :invalid_node_instance_id}
    end
  end

  defp fetch_valid_worker_role(opts) do
    case Keyword.get(opts, :worker_role) do
      value when is_atom(value) ->
        role = Atom.to_string(value)

        if Regex.match?(@node_shortname_regex, role) do
          {:ok, role}
        else
          {:error, :invalid_worker_role}
        end

      value when is_binary(value) and value != "" ->
        if Regex.match?(@node_shortname_regex, value) do
          {:ok, value}
        else
          {:error, :invalid_worker_role}
        end

      _other ->
        {:error, :invalid_worker_role}
    end
  end

  defp fetch_task_queue_hash(opts) do
    case Keyword.get(opts, :task_queue) do
      value when is_binary(value) and value != "" -> {:ok, task_queue_hash(value)}
      _other -> {:error, :invalid_task_queue}
    end
  end

  defp task_queue_hash(queue) do
    case Regex.run(@typed_queue_regex, queue) do
      [_, hash] -> hash
      nil -> hash_segment(queue)
    end
  end

  defp ensure_identity_size(identity) when byte_size(identity) <= 96, do: identity
  defp ensure_identity_size(_identity), do: {:error, :worker_identity_too_long}
end
