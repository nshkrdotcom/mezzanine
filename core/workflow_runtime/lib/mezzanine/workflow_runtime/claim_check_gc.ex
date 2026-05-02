defmodule Mezzanine.WorkflowRuntime.ClaimCheckGc do
  @moduledoc """
  Retained local Oban claim-check garbage-collection contract.

  Temporal retention does not own Postgres claim-check payload cleanup. This
  module keeps that duty bounded and local so it cannot become a hidden saga.
  """

  @queue :claim_check_gc
  @contract "Mezzanine.ClaimCheckGcLocalJob.v1"
  @normalizable_keys [
    :correlation_id,
    :installation_ref,
    :release_manifest_ref,
    :retention_policy_ref,
    :sweep_ref,
    :tenant_ref,
    :trace_id
  ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @doc "Static retained-local-job contract."
  @spec contract() :: map()
  def contract do
    %{
      contract_name: @contract,
      queue: @queue,
      worker_module: Mezzanine.WorkflowRuntime.ClaimCheckGcWorker,
      classification: :valid_claim_check_gc,
      permitted_actions: [
        :orphaned_blob_detection,
        :expired_payload_deletion,
        :audit_tombstone_write,
        :scratch_projection_cleanup
      ],
      forbidden_actions: [
        :workflow_state_transition,
        :activity_retry_policy,
        :human_review_wait,
        :fanout_fanin_coordination
      ]
    }
  end

  @doc "Normalize job args into a bounded local sweep plan."
  @spec sweep_plan(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def sweep_plan(attrs) do
    attrs = normalize(attrs)

    required = [
      :tenant_ref,
      :installation_ref,
      :retention_policy_ref,
      :trace_id,
      :correlation_id,
      :release_manifest_ref
    ]

    case missing(attrs, required) do
      [] ->
        {:ok,
         %{
           sweep_ref: Map.get(attrs, :sweep_ref, "claim-check-gc://#{attrs.trace_id}"),
           tenant_ref: attrs.tenant_ref,
           installation_ref: attrs.installation_ref,
           retention_policy_ref: attrs.retention_policy_ref,
           trace_id: attrs.trace_id,
           correlation_id: attrs.correlation_id,
           release_manifest_ref: attrs.release_manifest_ref,
           result: :bounded_local_gc_plan
         }}

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  defp normalize(attrs) when is_map(attrs) do
    attrs
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)

  defp missing(attrs, required), do: Enum.reject(required, &present?(attrs, &1))

  defp present?(attrs, key), do: Map.get(attrs, key) not in [nil, ""]
end

defmodule Mezzanine.WorkflowRuntime.ClaimCheckGcWorker do
  @moduledoc """
  Bounded local Oban worker for claim-check garbage collection.
  """

  use Oban.Worker, queue: :claim_check_gc, max_attempts: 3

  alias Mezzanine.WorkflowRuntime.ClaimCheckGc

  @impl true
  def perform(%Oban.Job{args: args}) do
    with {:ok, _plan} <- ClaimCheckGc.sweep_plan(args) do
      :ok
    end
  end

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      keys: [:tenant_ref, :installation_ref, :retention_policy_ref],
      states: [:available, :scheduled, :executing, :retryable],
      period: 86_400
    ]
  end
end
