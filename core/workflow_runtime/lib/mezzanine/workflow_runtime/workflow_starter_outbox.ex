defmodule Mezzanine.WorkflowRuntime.WorkflowStarterOutbox do
  @moduledoc """
  Phase 4 workflow-start outbox contract and dispatcher helpers.

  The outbox row is committed with the accepted command receipt and the Oban
  dispatcher job in one local transaction. The worker is only a post-commit
  dispatcher; it never owns workflow business state.
  """

  alias Mezzanine.WorkflowRuntime.DurableOrchestrationDecision
  alias Mezzanine.WorkflowStartOutboxPayload
  alias Mezzanine.WorkflowStartReceipt

  @table "workflow_start_outbox"
  @queue :workflow_start_outbox
  @contract "Mezzanine.WorkflowStarterOutbox.v1"
  @idempotency_contract "Mezzanine.WorkflowStartIdempotency.v1"
  @release_manifest_ref "phase4-v6-milestone26-durable-workflow-starter-outbox"

  @required_fields [
    :outbox_id,
    :tenant_ref,
    :installation_ref,
    :principal_ref,
    :resource_ref,
    :command_receipt_ref,
    :command_id,
    :workflow_type,
    :workflow_id,
    :workflow_version,
    :workflow_input_version,
    :workflow_input_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :dedupe_scope,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :payload_hash,
    :dispatch_state
  ]

  @operator_projection_fields [
    :outbox_id,
    :tenant_ref,
    :resource_ref,
    :workflow_type,
    :workflow_id,
    :workflow_version,
    :command_id,
    :dispatch_state,
    :retry_count,
    :last_error_class,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :oban_job_ref
  ]

  @dispatch_states [
    :queued,
    :dispatching,
    :started,
    :duplicate_started,
    :retryable_failure,
    :terminal_failure
  ]

  @failure_classes [
    :none,
    :duplicate_start_existing_workflow,
    :retryable_temporal_unavailable,
    :terminal_invalid_workflow_start,
    :terminal_conflicting_duplicate_start
  ]

  @doc "Static resource shape required by Scenario 91 and Scenario 92."
  @spec schema_contract() :: map()
  def schema_contract do
    %{
      contract_name: @contract,
      idempotency_contract_name: @idempotency_contract,
      table_name: @table,
      oban_queue: @queue,
      worker_module: Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker,
      runtime_boundary: Mezzanine.WorkflowRuntime,
      temporal_client_boundary: Mezzanine.WorkflowRuntime.TemporalexBoundary,
      required_fields: @required_fields,
      dispatch_states: @dispatch_states,
      failure_classes: @failure_classes,
      operator_projection_fields: @operator_projection_fields,
      release_manifest_ref: @release_manifest_ref
    }
  end

  @doc "Build a complete outbox row with Phase 4 default dispatch fields."
  @spec new_row(map() | keyword()) :: {:ok, WorkflowStartOutboxPayload.t()} | {:error, term()}
  def new_row(attrs) do
    attrs
    |> normalize()
    |> Map.put_new(:dispatch_state, "queued")
    |> Map.put_new(:retry_count, 0)
    |> Map.put_new(:release_manifest_ref, @release_manifest_ref)
    |> WorkflowStartOutboxPayload.new()
  end

  @doc "Transaction plan that must be executed atomically by the owning repo."
  @spec same_transaction_plan(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def same_transaction_plan(attrs) do
    with {:ok, row} <- new_row(attrs) do
      {:ok,
       %{
         transaction_boundary: :accepted_command_receipt_and_workflow_start_outbox,
         repo_owner: :mezzanine,
         operations: [
           %{
             op: :persist_accepted_command_receipt,
             ref: row.command_receipt_ref,
             required_before: :workflow_start_outbox_row
           },
           %{
             op: :insert_workflow_start_outbox_row,
             table: @table,
             outbox_id: row.outbox_id,
             workflow_id: row.workflow_id,
             idempotency_key: row.idempotency_key
           },
           %{
             op: :insert_oban_dispatch_job,
             queue: @queue,
             worker: Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker,
             args: dispatch_job_args(row),
             unique: unique_declaration()
           }
         ],
         forbidden_inside_transaction: [
           :temporalex_client_call,
           :direct_temporal_start,
           :workflow_business_logic
         ]
       }}
    end
  end

  @doc "Stable deterministic workflow id derived from the enterprise scope."
  @spec deterministic_workflow_id(map() | keyword()) :: String.t()
  def deterministic_workflow_id(attrs) do
    attrs = Map.new(attrs)

    [
      "tenant",
      fetch!(attrs, :tenant_ref),
      "resource",
      fetch!(attrs, :resource_ref),
      "workflow",
      fetch!(attrs, :workflow_type),
      "command",
      fetch!(attrs, :command_id),
      "release",
      fetch!(attrs, :release_manifest_ref)
    ]
    |> Enum.join(":")
  end

  @doc "Whether a second row is an exact idempotent duplicate of the first."
  @spec duplicate_start_safe?(map() | struct(), map() | struct()) :: boolean()
  def duplicate_start_safe?(left, right) do
    left = normalize(left)
    right = normalize(right)

    Enum.all?(
      [:tenant_ref, :resource_ref, :workflow_type, :workflow_id, :idempotency_key, :dedupe_scope],
      &(Map.get(left, &1) == Map.get(right, &1))
    )
  end

  @doc "Build the compact request consumed by Mezzanine.WorkflowRuntime.start_workflow/1."
  @spec start_request(map() | struct()) :: {:ok, map()} | {:error, term()}
  def start_request(row) do
    row = normalize(row)

    case missing_required(row) do
      [] ->
        {:ok,
         %{
           workflow_id: row.workflow_id,
           workflow_type: row.workflow_type,
           workflow_version: row.workflow_version,
           workflow_module: workflow_module(row.workflow_type),
           task_queue: task_queue(row.workflow_type),
           workflow_input_ref: row.workflow_input_ref,
           workflow_input_version: row.workflow_input_version,
           args: %{
             tenant_ref: row.tenant_ref,
             resource_ref: row.resource_ref,
             command_id: row.command_id,
             workflow_input_ref: row.workflow_input_ref,
             trace_id: row.trace_id,
             correlation_id: row.correlation_id,
             routing_facts: %{}
           },
           search_attributes: search_attributes(row),
           idempotency_key: row.idempotency_key,
           dedupe_scope: row.dedupe_scope,
           authority_packet_ref: row.authority_packet_ref,
           permission_decision_ref: row.permission_decision_ref,
           trace_id: row.trace_id,
           release_manifest_ref: row.release_manifest_ref
         }}

      missing ->
        {:error, {:missing_required_fields, missing}}
    end
  end

  @doc "Oban job args are compact refs and scalar routing facts only."
  @spec dispatch_job_args(map() | struct()) :: map()
  def dispatch_job_args(row) do
    row
    |> normalize()
    |> Map.take(@required_fields ++ [:payload_ref, :retry_count, :oban_job_ref])
    |> stringify_keys()
  end

  @doc "Unique declaration used by the Oban dispatcher."
  @spec unique_declaration() :: keyword()
  def unique_declaration do
    [
      keys: [:workflow_id, :idempotency_key],
      states: [:available, :scheduled, :executing, :retryable],
      period: :infinity
    ]
  end

  @doc "Classify the runtime start response without exposing SDK errors."
  @spec classify_start_result(
          map() | struct(),
          {:ok, WorkflowStartReceipt.t()} | {:error, term()}
        ) ::
          {:ok, map()} | {:retry, map()} | {:error, map()}
  def classify_start_result(row, result) do
    row = normalize(row)

    case result do
      {:ok, %WorkflowStartReceipt{} = receipt} ->
        {:ok,
         row
         |> Map.put(:dispatch_state, "started")
         |> Map.put(:workflow_run_id, receipt.workflow_run_id)
         |> Map.put(:last_error_class, "none")}

      {:error, {:already_started, existing_ref}} ->
        {:ok,
         row
         |> Map.put(:dispatch_state, "duplicate_started")
         |> Map.put(:workflow_run_id, existing_ref)
         |> Map.put(:last_error_class, "duplicate_start_existing_workflow")}

      {:error, {:conflict, reason}} ->
        {:error,
         row
         |> Map.put(:dispatch_state, "terminal_failure")
         |> Map.put(:last_error_class, {:terminal_conflicting_duplicate_start, reason})}

      {:error, {:invalid_request, reason}} ->
        {:error,
         row
         |> Map.put(:dispatch_state, "terminal_failure")
         |> Map.put(:last_error_class, {:terminal_invalid_workflow_start, reason})}

      {:error, reason} ->
        {:retry,
         row
         |> Map.put(:dispatch_state, "retryable_failure")
         |> Map.put(:retry_count, Map.get(row, :retry_count, 0) + 1)
         |> Map.put(:last_error_class, {:retryable_temporal_unavailable, reason})}
    end
  end

  @doc "Fields that make stuck starter rows visible to the operator surface."
  @spec operator_projection(map() | struct()) :: map()
  def operator_projection(row), do: row |> normalize() |> Map.take(@operator_projection_fields)

  @doc "Fields required in incident bundles for lost-start reconstruction."
  @spec incident_bundle_fields(map() | struct()) :: map()
  def incident_bundle_fields(row) do
    row
    |> normalize()
    |> Map.take([
      :outbox_id,
      :tenant_ref,
      :resource_ref,
      :command_receipt_ref,
      :workflow_id,
      :workflow_type,
      :workflow_version,
      :dispatch_state,
      :last_error_class,
      :trace_id,
      :correlation_id,
      :release_manifest_ref,
      :oban_job_ref
    ])
  end

  defp search_attributes(row) do
    %{
      "phase4.tenant_ref" => row.tenant_ref,
      "phase4.resource_ref" => row.resource_ref,
      "phase4.workflow_type" => row.workflow_type,
      "phase4.workflow_version" => row.workflow_version,
      "phase4.command_id" => row.command_id,
      "phase4.trace_id" => row.trace_id,
      "phase4.idempotency_key_hash" =>
        Base.encode16(:crypto.hash(:sha256, row.idempotency_key), case: :lower),
      "phase4.release_manifest_ref" => row.release_manifest_ref
    }
  end

  defp workflow_module("agent_run"), do: Mezzanine.Workflows.AgentRun
  defp workflow_module("execution_attempt"), do: Mezzanine.Workflows.ExecutionAttempt
  defp workflow_module("decision_review"), do: Mezzanine.Workflows.DecisionReview
  defp workflow_module("join_barrier"), do: Mezzanine.Workflows.JoinBarrier
  defp workflow_module("incident_reconstruction"), do: Mezzanine.Workflows.IncidentReconstruction
  defp workflow_module(_workflow_type), do: Mezzanine.Workflows.AgentRun

  defp task_queue(workflow_type) do
    DurableOrchestrationDecision.workflow_types()
    |> Enum.find(%{task_queue: "mezzanine.agentic"}, &(&1.name == workflow_name(workflow_type)))
    |> Map.fetch!(:task_queue)
  end

  defp workflow_name(workflow_type) when is_binary(workflow_type) do
    workflow_type
    |> String.replace("-", "_")
    |> String.to_existing_atom()
  rescue
    ArgumentError -> :agent_run
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_keys()
  defp normalize(%_{} = struct), do: Map.from_struct(struct)
  defp normalize(map) when is_map(map), do: normalize_keys(map)

  defp normalize_keys(map) do
    map
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: String.to_existing_atom(key)

  defp missing_required(row), do: Enum.reject(@required_fields, &present?(Map.get(row, &1)))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)

  defp fetch!(attrs, key) do
    Map.get(attrs, key) || Map.fetch!(attrs, to_string(key))
  end

  defp stringify_keys(map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), value} end)
    |> Map.new()
  end
end

defmodule Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker do
  @moduledoc """
  Oban-backed workflow start dispatcher.

  This worker has no workflow business logic. It turns a committed starter
  outbox row into a `Mezzanine.WorkflowRuntime.start_workflow/1` call.
  """

  use Oban.Worker, queue: :workflow_start_outbox, max_attempts: 20

  alias Mezzanine.WorkflowRuntime.WorkflowStarterOutbox

  @impl true
  def perform(%Oban.Job{args: args}) do
    with {:ok, request} <- WorkflowStarterOutbox.start_request(args),
         result <- Mezzanine.WorkflowRuntime.start_workflow(request) do
      case WorkflowStarterOutbox.classify_start_result(args, result) do
        {:ok, _row} -> :ok
        {:retry, _row} -> {:snooze, 30}
        {:error, row} -> {:error, Map.fetch!(row, :last_error_class)}
      end
    end
  end

  @doc "Unique Oban declaration used by Mezzanine.JobOutbox.Oban."
  @spec unique_declaration() :: keyword()
  def unique_declaration, do: WorkflowStarterOutbox.unique_declaration()
end
