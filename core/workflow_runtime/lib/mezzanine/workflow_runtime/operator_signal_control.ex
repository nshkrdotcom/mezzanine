defmodule Mezzanine.WorkflowRuntime.OperatorSignalControl do
  @moduledoc """
  Durable decision-timer and operator workflow-signal control for Phase 4 M28.

  The module models the implementation contract without letting AppKit or other
  product code talk to Temporal directly. Operator actions become locally
  accepted signal intents, Oban-owned signal outbox rows dispatch through
  `Mezzanine.WorkflowRuntime.signal_workflow/1`, and workflow handler
  acknowledgements are required before projections claim a processed effect.
  """

  alias Mezzanine.OperatorWorkflowSignal
  alias Mezzanine.WorkflowDecisionTimer
  alias Mezzanine.WorkflowRuntime.OutboxPersistence
  alias Mezzanine.WorkflowSignalAcknowledgement
  alias Mezzanine.WorkflowSignalOutboxRow
  alias Mezzanine.WorkflowSignalReceipt

  @release_manifest_ref "phase4-v6-milestone28-decision-timer-operator-signal-control"

  @operator_signal_registry [
    %{
      signal_name: "operator.cancel",
      signal_version: "operator-cancel.v1",
      signal_effect: "cancel_requested",
      handler: :handle_operator_cancel,
      terminal?: true
    },
    %{
      signal_name: "operator.pause",
      signal_version: "operator-pause.v1",
      signal_effect: "pause_requested",
      handler: :handle_operator_pause,
      terminal?: false
    },
    %{
      signal_name: "operator.resume",
      signal_version: "operator-resume.v1",
      signal_effect: "resume_requested",
      handler: :handle_operator_resume,
      terminal?: false
    },
    %{
      signal_name: "operator.retry",
      signal_version: "operator-retry.v1",
      signal_effect: "retry_requested",
      handler: :handle_operator_retry,
      terminal?: false
    },
    %{
      signal_name: "operator.replan",
      signal_version: "operator-replan.v1",
      signal_effect: "replan_requested",
      handler: :handle_operator_replan,
      terminal?: false
    },
    %{
      signal_name: "operator.rework",
      signal_version: "operator-rework.v1",
      signal_effect: "rework_requested",
      handler: :handle_operator_rework,
      terminal?: false
    }
  ]

  @doc "Contracts and handlers owned by M28."
  @spec contract() :: map()
  def contract do
    %{
      decision_timer_contract: "Mezzanine.WorkflowDecisionTimer.v1",
      operator_signal_contract: "Mezzanine.OperatorWorkflowSignal.v1",
      signal_outbox_contract: "Mezzanine.WorkflowSignalOutboxRow.v1",
      acknowledgement_contract: "Mezzanine.WorkflowSignalAcknowledgement.v1",
      appkit_result_contract: "AppKit.OperatorSignalResult.v1",
      citadel_authority_contract: "Citadel.OperatorWorkflowSignalAuthority.v1",
      signal_boundary: Mezzanine.WorkflowRuntime,
      signal_outbox_queue: :workflow_signal_outbox,
      outbox_persistence_boundary: OutboxPersistence,
      signal_registry: @operator_signal_registry,
      release_manifest_ref: @release_manifest_ref
    }
  end

  @doc "Signal names, versions, and handler modules registered for workflows."
  @spec signal_registry() :: [map()]
  def signal_registry, do: @operator_signal_registry

  @doc "Returns true only for registered signal name/version pairs."
  @spec registered_signal?(String.t(), String.t()) :: boolean()
  def registered_signal?(signal_name, signal_version) do
    Enum.any?(@operator_signal_registry, fn entry ->
      entry.signal_name == signal_name and entry.signal_version == signal_version
    end)
  end

  @doc "Creates the durable workflow timer input for decision expiry."
  @spec decision_timer(map() | keyword()) :: {:ok, WorkflowDecisionTimer.t()} | {:error, term()}
  def decision_timer(attrs), do: attrs |> with_release_ref() |> WorkflowDecisionTimer.new()

  @doc "Runs the decision-review workflow to its timer wait point."
  @spec run_decision_review(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def run_decision_review(attrs) do
    with {:ok, timer} <- decision_timer(attrs) do
      {:ok,
       %{
         workflow_id: timer.workflow_id,
         workflow_run_id: timer.workflow_run_id,
         workflow_state: "awaiting_decision_or_timer",
         timer_ref: "workflow-timer://#{timer.workflow_id}/#{timer.timer_id}",
         timer_id: timer.timer_id,
         timer_state: timer.timer_state,
         timer_duration_ms: timer.timer_duration_ms,
         workflow_history_ref: timer.workflow_history_ref,
         projection_ref: timer.projection_ref,
         trace_id: timer.trace_id,
         release_manifest_ref: timer.release_manifest_ref,
         history_policy: "temporal_timer_history"
       }}
    end
  end

  @doc "Accepts or rejects an operator signal locally before outbox dispatch."
  @spec accept_operator_signal(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def accept_operator_signal(attrs) do
    attrs = with_release_ref(attrs)

    with {:ok, signal} <- build_signal(attrs),
         :ok <- ensure_registered(signal) do
      accept_registered_signal(signal, authorized?(attrs))
    end
  end

  defp accept_registered_signal(signal, true) do
    with {:ok, receipt} <-
           signal_receipt(signal, "authorized", "accepted", "queued", "pending", "lagging"),
         {:ok, outbox} <- signal_outbox(signal, receipt) do
      {:ok, %{signal: signal, receipt: receipt, outbox: outbox}}
    end
  end

  defp accept_registered_signal(signal, false) do
    with {:ok, receipt} <-
           signal_receipt(
             signal,
             "denied",
             "rejected",
             "not_dispatched",
             "rejected_by_authority",
             "fresh"
           ) do
      {:ok, %{signal: signal, receipt: receipt}}
    end
  end

  @doc "Dispatches an already committed local signal intent through WorkflowRuntime."
  @spec dispatch_operator_signal(map()) :: {:ok, map()} | {:error, term()}
  def dispatch_operator_signal(%{signal: %OperatorWorkflowSignal{} = signal, receipt: receipt}) do
    with request <- signal_runtime_request(signal),
         {:ok, runtime_receipt} <- Mezzanine.WorkflowRuntime.signal_workflow(request),
         {:ok, delivered_receipt} <-
           signal_receipt(
             signal,
             receipt.authority_state,
             receipt.local_state,
             "delivered_to_temporal",
             "pending",
             "lagging"
           ) do
      {:ok,
       %{
         signal: signal,
         receipt: delivered_receipt,
         runtime_receipt: sanitize_runtime_receipt(runtime_receipt)
       }}
    end
  end

  @doc "Classifies a retained signal-outbox Temporal dispatch outcome."
  @spec classify_signal_result(map() | struct(), {:ok, term()} | {:error, term()}) ::
          {:ok, map()} | {:retry, map()} | {:error, map()}
  def classify_signal_result(row, result) do
    row =
      row
      |> normalize()
      |> Map.update(:dispatch_attempt_count, 1, &(&1 + 1))

    case result do
      {:ok, runtime_receipt} ->
        receipt = sanitize_runtime_receipt(runtime_receipt)

        {:ok,
         row
         |> Map.put(
           :dispatch_state,
           receipt[:dispatch_state] || receipt[:status] || "delivered_to_temporal"
         )
         |> Map.put(:workflow_effect_state, receipt[:workflow_effect_state] || "pending_ack")
         |> Map.put(:projection_state, receipt[:projection_state] || "pending")
         |> Map.put(:last_error_class, "none")}

      {:error, :workflow_runtime_unconfigured} ->
        {:retry,
         row
         |> Map.put(:dispatch_state, "retryable_failure")
         |> Map.put(:workflow_effect_state, "pending")
         |> Map.put(:projection_state, "lagging")
         |> Map.put(:last_error_class, "workflow_runtime_unconfigured")}

      {:error, {:invalid_request, reason}} ->
        {:error,
         row
         |> Map.put(:dispatch_state, "terminal_failure")
         |> Map.put(:workflow_effect_state, "not_delivered")
         |> Map.put(:projection_state, "fresh")
         |> Map.put(:last_error_class, {:terminal_invalid_workflow_signal, reason})}

      {:error, reason} ->
        {:retry,
         row
         |> Map.put(:dispatch_state, "retryable_failure")
         |> Map.put(:workflow_effect_state, "pending")
         |> Map.put(:projection_state, "lagging")
         |> Map.put(:last_error_class, {:retryable_temporal_signal_failure, reason})}
    end
  end

  @doc "Projects a workflow-emitted acknowledgement into public-safe receipt state."
  @spec apply_workflow_ack(WorkflowSignalReceipt.t(), map() | keyword()) ::
          {:ok, map()} | {:error, term()}
  def apply_workflow_ack(%WorkflowSignalReceipt{} = receipt, attrs) do
    attrs = normalize(attrs)
    registry = registry_entry!(receipt.signal_name, receipt.signal_version)

    with {:ok, ack} <-
           WorkflowSignalAcknowledgement.new(%{
             tenant_ref: receipt.tenant_ref,
             installation_ref: receipt.installation_ref,
             workspace_ref: receipt.workspace_ref,
             project_ref: receipt.project_ref,
             environment_ref: receipt.environment_ref,
             principal_ref: receipt.principal_ref,
             system_actor_ref: receipt.system_actor_ref,
             operator_ref: receipt.operator_ref,
             resource_ref: receipt.resource_ref,
             workflow_id: receipt.workflow_id,
             workflow_run_id: receipt.workflow_run_id,
             signal_id: receipt.signal_id,
             signal_name: receipt.signal_name,
             signal_version: receipt.signal_version,
             signal_sequence: Map.get(attrs, :signal_sequence, receipt.signal_sequence || 0),
             signal_effect: registry.signal_effect,
             workflow_effect_state: "processed_by_workflow",
             workflow_event_ref: Map.fetch!(attrs, :workflow_event_ref),
             authority_packet_ref: receipt.authority_packet_ref,
             permission_decision_ref: receipt.permission_decision_ref,
             idempotency_key: receipt.idempotency_key,
             trace_id: receipt.trace_id,
             correlation_id: receipt.correlation_id,
             release_manifest_ref: receipt.release_manifest_ref,
             acknowledged_at: Map.fetch!(attrs, :acknowledged_at)
           }),
         {:ok, projected_receipt} <-
           receipt
           |> Map.from_struct()
           |> Map.merge(%{
             workflow_effect_state: "processed_by_workflow",
             projection_state: "fresh",
             workflow_ack_event_ref: ack.workflow_event_ref,
             workflow_acknowledged_at: ack.acknowledged_at
           })
           |> WorkflowSignalReceipt.new() do
      {:ok, %{receipt: projected_receipt, ack: ack}}
    end
  end

  @doc "Initial deterministic state for pause/resume ordering proof."
  @spec initial_ordering_state() :: map()
  def initial_ordering_state do
    %{
      workflow_mode: "running",
      last_signal_sequence: 0,
      seen_signal_keys: MapSet.new(),
      ordering_state: "ready"
    }
  end

  @doc "Applies pause/resume/retry/replan/rework/cancel signal ordering deterministically."
  @spec apply_ordered_signal(map(), map() | keyword()) :: {:ok, map()} | {:error, term()}
  def apply_ordered_signal(state, attrs) when is_map(state) do
    with {:ok, signal} <- attrs |> with_release_ref() |> build_signal(),
         :ok <- ensure_registered(signal) do
      seen = Map.get(state, :seen_signal_keys, MapSet.new())

      cond do
        MapSet.member?(seen, signal.idempotency_key) ->
          {:ok, Map.put(state, :ordering_state, "duplicate_suppressed")}

        signal.signal_sequence <= Map.get(state, :last_signal_sequence, 0) ->
          {:ok, Map.put(state, :ordering_state, "out_of_order_rejected")}

        signal.signal_name == "operator.resume" and Map.get(state, :workflow_mode) != "paused" ->
          {:ok, Map.put(state, :ordering_state, "resume_without_pause_rejected")}

        true ->
          {:ok,
           state
           |> Map.put(:workflow_mode, workflow_mode_after(signal))
           |> Map.put(:last_signal_sequence, signal.signal_sequence)
           |> Map.put(:seen_signal_keys, MapSet.put(seen, signal.idempotency_key))
           |> Map.put(:ordering_state, "applied")}
      end
    end
  end

  @doc "Public-safe operator signal result shape used by AppKit projections."
  @spec operator_signal_result(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def operator_signal_result(attrs) do
    attrs = normalize(attrs)

    required = [
      :command_id,
      :signal_id,
      :workflow_ref,
      :tenant_ref,
      :installation_ref,
      :operator_ref,
      :resource_ref,
      :authority_packet_ref,
      :permission_decision_ref,
      :idempotency_key,
      :authority_state,
      :local_state,
      :dispatch_state,
      :workflow_effect_state,
      :projection_state,
      :trace_id,
      :correlation_id,
      :release_manifest_version
    ]

    case missing_required(attrs, required) do
      [] ->
        {:ok,
         attrs
         |> Map.take(
           required ++
             [
               :incident_bundle_ref,
               :last_projection_event_ref,
               :operator_message,
               :retry_after_ms,
               :staleness_started_at
             ]
         )
         |> Map.put_new(:operator_message, operator_message(attrs))
         |> Map.put_new(:bounded_wait_source, "postgres_projection_only")}

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  defp build_signal(attrs) do
    attrs
    |> normalize()
    |> put_signal_effect()
    |> OperatorWorkflowSignal.new()
  end

  defp put_signal_effect(attrs) do
    case Enum.find(@operator_signal_registry, &(&1.signal_name == Map.get(attrs, :signal_name))) do
      nil -> attrs
      registry -> Map.put_new(attrs, :signal_effect, registry.signal_effect)
    end
  end

  defp ensure_registered(%OperatorWorkflowSignal{} = signal) do
    if registered_signal?(signal.signal_name, signal.signal_version) do
      :ok
    else
      {:error, {:unregistered_signal, signal.signal_name, signal.signal_version}}
    end
  end

  defp authorized?(attrs) do
    Map.get(attrs, :permission_decision_result, "allow") in [
      "allow",
      :allow,
      "authorized",
      :authorized
    ]
  end

  defp signal_receipt(
         %OperatorWorkflowSignal{} = signal,
         authority_state,
         local_state,
         dispatch_state,
         workflow_effect_state,
         projection_state
       ) do
    WorkflowSignalReceipt.new(%{
      tenant_ref: signal.tenant_ref,
      installation_ref: signal.installation_ref,
      workspace_ref: signal.workspace_ref,
      project_ref: signal.project_ref,
      environment_ref: signal.environment_ref,
      principal_ref: signal.principal_ref,
      system_actor_ref: signal.system_actor_ref,
      operator_ref: signal.operator_ref,
      resource_ref: signal.resource_ref,
      signal_id: signal.signal_id,
      workflow_id: signal.workflow_id,
      workflow_run_id: signal.workflow_run_id,
      signal_name: signal.signal_name,
      signal_version: signal.signal_version,
      signal_sequence: signal.signal_sequence,
      command_id: signal.signal_id,
      authority_packet_ref: signal.authority_packet_ref,
      permission_decision_ref: signal.permission_decision_ref,
      idempotency_key: signal.idempotency_key,
      trace_id: signal.trace_id,
      correlation_id: signal.correlation_id,
      release_manifest_ref: signal.release_manifest_ref,
      payload_hash: signal.payload_hash,
      payload_ref: signal.payload_ref,
      authority_state: authority_state,
      local_state: local_state,
      dispatch_state: dispatch_state,
      workflow_effect_state: workflow_effect_state,
      projection_state: projection_state,
      dispatch_attempt_count: 0
    })
  end

  defp signal_outbox(%OperatorWorkflowSignal{} = signal, %WorkflowSignalReceipt{} = receipt) do
    WorkflowSignalOutboxRow.new(%{
      outbox_id: "signal-outbox://#{signal.workflow_id}/#{signal.signal_id}",
      tenant_ref: signal.tenant_ref,
      installation_ref: signal.installation_ref,
      workspace_ref: signal.workspace_ref,
      project_ref: signal.project_ref,
      environment_ref: signal.environment_ref,
      principal_ref: signal.principal_ref,
      system_actor_ref: signal.system_actor_ref,
      operator_ref: signal.operator_ref,
      resource_ref: signal.resource_ref,
      signal_id: signal.signal_id,
      workflow_id: signal.workflow_id,
      workflow_run_id: signal.workflow_run_id,
      signal_name: signal.signal_name,
      signal_version: signal.signal_version,
      signal_sequence: signal.signal_sequence,
      authority_packet_ref: signal.authority_packet_ref,
      permission_decision_ref: signal.permission_decision_ref,
      idempotency_key: signal.idempotency_key,
      trace_id: signal.trace_id,
      correlation_id: signal.correlation_id,
      release_manifest_ref: signal.release_manifest_ref,
      dispatch_state: receipt.dispatch_state,
      workflow_effect_state: receipt.workflow_effect_state,
      projection_state: receipt.projection_state,
      available_at: "after_local_commit",
      dispatch_attempt_count: 0,
      oban_job_ref: "oban://workflow_signal_outbox/#{signal.signal_id}"
    })
  end

  defp signal_runtime_request(%OperatorWorkflowSignal{} = signal) do
    %{
      workflow_id: signal.workflow_id,
      workflow_run_id: signal.workflow_run_id,
      signal_id: signal.signal_id,
      signal_name: signal.signal_name,
      signal_version: signal.signal_version,
      signal_payload_ref: signal.payload_ref,
      signal_payload_hash: signal.payload_hash,
      idempotency_key: signal.idempotency_key,
      tenant_ref: signal.tenant_ref,
      resource_ref: signal.resource_ref,
      authority_packet_ref: signal.authority_packet_ref,
      permission_decision_ref: signal.permission_decision_ref,
      trace_id: signal.trace_id,
      correlation_id: signal.correlation_id,
      release_manifest_ref: signal.release_manifest_ref
    }
  end

  defp registry_entry!(signal_name, signal_version) do
    Enum.find(@operator_signal_registry, fn entry ->
      entry.signal_name == signal_name and entry.signal_version == signal_version
    end) || raise ArgumentError, "unregistered workflow signal #{signal_name}@#{signal_version}"
  end

  defp workflow_mode_after(%OperatorWorkflowSignal{signal_name: "operator.pause"}), do: "paused"
  defp workflow_mode_after(%OperatorWorkflowSignal{signal_name: "operator.resume"}), do: "running"

  defp workflow_mode_after(%OperatorWorkflowSignal{signal_name: "operator.cancel"}),
    do: "cancel_requested"

  defp workflow_mode_after(%OperatorWorkflowSignal{signal_name: "operator.retry"}),
    do: "retry_requested"

  defp workflow_mode_after(%OperatorWorkflowSignal{signal_name: "operator.replan"}),
    do: "replan_requested"

  defp workflow_mode_after(%OperatorWorkflowSignal{signal_name: "operator.rework"}),
    do: "rework_requested"

  defp operator_message(%{workflow_effect_state: "pending"}),
    do: "workflow effect pending; awaiting workflow acknowledgement projection"

  defp operator_message(%{workflow_effect_state: "processed_by_workflow"}),
    do: "workflow processed operator signal"

  defp operator_message(%{workflow_effect_state: state}), do: "workflow effect #{state}"

  defp sanitize_runtime_receipt(receipt) do
    receipt
    |> normalize()
    |> Map.drop([:raw_temporalex_result, :temporalex_struct, :raw_history_event, :task_token])
  end

  defp with_release_ref(attrs) do
    attrs
    |> normalize()
    |> Map.put_new(:release_manifest_ref, @release_manifest_ref)
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_keys()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize_keys()
  defp normalize(map) when is_map(map), do: normalize_keys(map)

  defp normalize_keys(map) do
    map
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: String.to_existing_atom(key)

  defp missing_required(attrs, required), do: Enum.reject(required, &present?(Map.get(attrs, &1)))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value), do: not is_nil(value)
end

defmodule Mezzanine.WorkflowRuntime.WorkflowSignalOutboxWorker do
  @moduledoc """
  Retained local Oban dispatcher for committed workflow-signal intents.

  The worker is an outbox dispatcher only. It dispatches an already-authorized
  local signal record through `Mezzanine.WorkflowRuntime.signal_workflow/1` and
  never owns workflow business state.
  """

  use Oban.Worker, queue: :workflow_signal_outbox, max_attempts: 20

  alias Mezzanine.WorkflowRuntime.{OperatorSignalControl, OutboxPersistence}

  @impl true
  def perform(%Oban.Job{args: args}) do
    result = Mezzanine.WorkflowRuntime.signal_workflow(signal_request(args))

    case OperatorSignalControl.classify_signal_result(args, result) do
      {:ok, row} ->
        persist_signal_outcome(args, row, :ok)

      {:retry, row} ->
        persist_signal_outcome(args, row, {:snooze, 30})

      {:error, row} ->
        persist_signal_outcome(args, row, {:error, Map.fetch!(row, :last_error_class)})
    end
  end

  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      keys: [:signal_id, :workflow_id, :idempotency_key],
      states: [:available, :scheduled, :executing, :retryable],
      period: :infinity
    ]
  end

  defp signal_request(args) do
    args
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: String.to_atom(key)

  defp persist_signal_outcome(args, row, worker_result) do
    case OutboxPersistence.record_signal_outcome(args, row) do
      :ok -> worker_result
      {:error, reason} -> {:error, {:outbox_outcome_not_persisted, reason}}
    end
  end
end
