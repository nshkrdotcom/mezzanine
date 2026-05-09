defmodule Mezzanine.M1M2Runtime.WorkflowStartHandoff do
  @moduledoc """
  Joins accepted WorkControl runs to the workflow-start outbox.

  WorkControl owns run acceptance. WorkflowRuntime owns post-commit dispatch.
  M1/M2 Runtime owns this join because it is the allowed package that can see
  both accepted execution state and workflow-runtime contracts. This service
  builds and persists the compact outbox row without calling Temporal or lower
  runtime code.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Audit.WorkAudit
  alias Mezzanine.Execution.Repo
  alias Mezzanine.WorkflowRuntime.WorkflowStarterOutbox

  @workflow_type "agent_run"
  @workflow_version "agent-run.v1"
  @workflow_input_version "run-start.v1"

  @insert_outbox_sql """
  INSERT INTO workflow_start_outbox (
    outbox_id,
    tenant_ref,
    installation_ref,
    principal_ref,
    resource_ref,
    command_receipt_ref,
    command_id,
    workflow_type,
    workflow_id,
    workflow_version,
    workflow_input_version,
    workflow_input_ref,
    authority_packet_ref,
    permission_decision_ref,
    idempotency_key,
    dedupe_scope,
    trace_id,
    correlation_id,
    release_manifest_ref,
    payload_hash,
    payload_ref,
    dispatch_state,
    retry_count,
    last_error_class,
    inserted_at,
    updated_at
  )
  VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18,
    $19, $20, $21, $22, $23, $24, now(), now()
  )
  ON CONFLICT (outbox_id) DO UPDATE
  SET updated_at = workflow_start_outbox.updated_at
  RETURNING outbox_id
  """

  @type started_run :: %{
          required(:work_object) => struct(),
          required(:plan) => struct(),
          required(:run) => struct(),
          optional(:review_unit) => struct() | nil
        }

  @spec enqueue_start(String.t(), started_run(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def enqueue_start(tenant_id, started_run, attrs, opts \\ [])
      when is_binary(tenant_id) and is_map(started_run) do
    attrs = normalize(attrs)

    with {:ok, row} <- outbox_row(tenant_id, started_run, attrs),
         {:ok, plan} <- WorkflowStarterOutbox.same_transaction_plan(row),
         {:ok, persisted} <- persist_outbox_row(row, opts),
         {:ok, evidence_ref} <- record_workflow_start_event(tenant_id, started_run, row) do
      {:ok,
       %{
         outbox_row: row,
         persisted_outbox_id: persisted.outbox_id,
         plan: plan,
         workflow_start_ref: workflow_start_ref(row),
         evidence_ref: evidence_ref
       }}
    end
  end

  @spec outbox_row(String.t(), started_run(), map() | keyword()) ::
          {:ok, Mezzanine.WorkflowStartOutboxPayload.t()} | {:error, term()}
  def outbox_row(tenant_id, started_run, attrs)
      when is_binary(tenant_id) and is_map(started_run) do
    attrs = normalize(attrs)

    with {:ok, trace_id} <- required_string(attrs, :trace_id) do
      idempotency_key =
        map_value(attrs, :idempotency_key) || default_idempotency_key(started_run)

      row_attrs = row_attrs(tenant_id, started_run, attrs, idempotency_key, trace_id)

      row_attrs
      |> Map.put(:workflow_id, WorkflowStarterOutbox.deterministic_workflow_id(row_attrs))
      |> put_outbox_identity()
      |> WorkflowStarterOutbox.new_row()
    end
  end

  @spec workflow_start_ref(map() | struct()) :: String.t()
  def workflow_start_ref(row) do
    "workflow-start-outbox://#{map_value(row, :outbox_id)}"
  end

  defp row_attrs(tenant_id, started_run, attrs, idempotency_key, trace_id) do
    work_object = Map.fetch!(started_run, :work_object)
    run = Map.fetch!(started_run, :run)
    release_manifest_ref = WorkflowStarterOutbox.schema_contract().release_manifest_ref
    command_id = "run:#{run.id}"
    resource_ref = resource_ref(work_object)

    %{
      tenant_ref: tenant_id,
      installation_ref: installation_ref(attrs, tenant_id),
      principal_ref: principal_ref(attrs),
      resource_ref: resource_ref,
      command_receipt_ref: "command-receipt://mezzanine/work-control/#{run.id}",
      command_id: command_id,
      workflow_type: workflow_type(attrs),
      workflow_version: workflow_version(attrs),
      workflow_input_version: workflow_input_version(attrs),
      workflow_input_ref: "workflow-input://mezzanine/run/#{run.id}",
      authority_packet_ref: authority_packet_ref(attrs, run),
      permission_decision_ref: permission_decision_ref(attrs, run),
      idempotency_key: idempotency_key,
      dedupe_scope: "#{tenant_id}:#{resource_ref}:#{command_id}",
      trace_id: trace_id,
      correlation_id: correlation_id(attrs, trace_id),
      release_manifest_ref: release_manifest_ref,
      payload_hash: payload_hash(started_run, attrs),
      payload_ref: "claim://workflow-start/#{run.id}",
      canonical_idempotency_key: map_value(attrs, :canonical_idempotency_key),
      causation_id: map_value(attrs, :causation_id),
      client_retry_key: map_value(attrs, :client_retry_key),
      platform_envelope_idempotency_key: map_value(attrs, :platform_envelope_idempotency_key)
    }
  end

  defp installation_ref(attrs, tenant_id),
    do: map_value(attrs, :installation_ref) || "installation://#{tenant_id}/default"

  defp principal_ref(attrs), do: map_value(attrs, :actor_ref) || "system://work-control"

  defp workflow_type(attrs), do: map_value(attrs, :workflow_type) || @workflow_type

  defp workflow_version(attrs), do: map_value(attrs, :workflow_version) || @workflow_version

  defp workflow_input_version(attrs),
    do: map_value(attrs, :workflow_input_version) || @workflow_input_version

  defp authority_packet_ref(attrs, run),
    do: map_value(attrs, :authority_packet_ref) || "authority-packet://pending/#{run.id}"

  defp permission_decision_ref(attrs, run),
    do: map_value(attrs, :permission_decision_ref) || "permission-decision://pending/#{run.id}"

  defp correlation_id(attrs, trace_id),
    do: map_value(attrs, :correlation_id) || map_value(attrs, :causation_id) || trace_id

  defp put_outbox_identity(%{workflow_id: workflow_id, idempotency_key: idempotency_key} = attrs) do
    Map.put(attrs, :outbox_id, "workflow-start:#{hash_ref("#{workflow_id}:#{idempotency_key}")}")
  end

  defp persist_outbox_row(row, opts) do
    repo = Keyword.get(opts, :repo, Repo)
    row = Map.from_struct(row)

    params = [
      row.outbox_id,
      row.tenant_ref,
      row.installation_ref,
      row.principal_ref,
      row.resource_ref,
      row.command_receipt_ref,
      row.command_id,
      row.workflow_type,
      row.workflow_id,
      row.workflow_version,
      row.workflow_input_version,
      row.workflow_input_ref,
      row.authority_packet_ref,
      row.permission_decision_ref,
      row.idempotency_key,
      row.dedupe_scope,
      row.trace_id,
      row.correlation_id,
      row.release_manifest_ref,
      row.payload_hash,
      row.payload_ref,
      row.dispatch_state,
      row.retry_count,
      Map.get(row, :last_error_class) || "none"
    ]

    case SQL.query(repo, @insert_outbox_sql, params) do
      {:ok, %{rows: [[outbox_id]]}} -> {:ok, %{outbox_id: outbox_id}}
      {:error, reason} -> {:error, {:workflow_start_outbox_insert_failed, reason}}
    end
  end

  defp record_workflow_start_event(tenant_id, started_run, row) do
    work_object = Map.fetch!(started_run, :work_object)
    run = Map.fetch!(started_run, :run)
    review_unit = Map.get(started_run, :review_unit)

    case WorkAudit.record_event(tenant_id, %{
           program_id: work_object.program_id,
           work_object_id: work_object.id,
           run_id: run.id,
           review_unit_id: review_unit && review_unit.id,
           event_kind: :workflow_start_queued,
           actor_kind: :system,
           actor_ref: "workflow_start_handoff",
           payload: %{
             workflow_start_ref: workflow_start_ref(row),
             outbox_id: row.outbox_id,
             workflow_id: row.workflow_id,
             idempotency_key: row.idempotency_key,
             trace_id: row.trace_id,
             dispatch_state: row.dispatch_state
           }
         }) do
      {:ok, event} -> {:ok, "audit-event://#{event.id}"}
      {:error, reason} -> {:error, {:workflow_start_evidence_failed, reason}}
    end
  end

  defp payload_hash(started_run, attrs) do
    payload = %{
      work_object_id: Map.fetch!(started_run, :work_object).id,
      plan_id: Map.fetch!(started_run, :plan).id,
      run_id: Map.fetch!(started_run, :run).id,
      runtime_profile: Map.fetch!(started_run, :run).runtime_profile,
      grant_profile: Map.fetch!(started_run, :run).grant_profile,
      attrs: attrs
    }

    payload
    |> Jason.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp resource_ref(work_object), do: "work-object://#{work_object.id}"

  defp default_idempotency_key(started_run) do
    "mezzanine-run:#{Map.fetch!(started_run, :run).id}"
  end

  defp hash_ref(value) do
    value
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp required_string(attrs, key) do
    case map_value(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:missing_required_handoff_field, key}}
    end
  end

  defp normalize(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize()
  defp normalize(attrs) when is_map(attrs), do: attrs

  defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)
  defp map_value(map, key) when is_map(map), do: Map.get(map, key) || Map.get(map, to_string(key))
end
