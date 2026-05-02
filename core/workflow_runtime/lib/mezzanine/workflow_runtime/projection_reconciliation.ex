defmodule Mezzanine.WorkflowRuntime.ProjectionReconciliation do
  @moduledoc """
  Phase 5 Temporal/Postgres reconciliation profile.

  This module is deliberately read-only. It defines the candidate SQL, compact
  Temporal lookup shape, drift actions, workflow-start outbox drain gate, and
  execution dispatch-state reduction profile used before any code path may
  mutate workflow-master state.
  """

  @release_manifest_ref "phase5-v7-milestone2-temporal-postgres-reconciliation"
  @query_name "execution.lifecycle_state"

  @postgres_active_states [
    "queued",
    "in_flight",
    "accepted_active"
  ]

  @postgres_terminal_states [
    "completed",
    "cancelled",
    "failed",
    "rejected"
  ]

  @temporal_active_states [
    "running",
    "open",
    "active",
    "accepted_active",
    "pending",
    "started"
  ]

  @temporal_terminal_states [
    "completed",
    "cancelled",
    "canceled",
    "failed",
    "terminated",
    "timed_out",
    "timeout",
    "closed",
    "succeeded"
  ]

  @profile_fields [
    :workflow_id,
    :workflow_type,
    :workflow_version,
    :workflow_run_id,
    :postgres_projection_owner,
    :projection_row_ids,
    :last_observed_workflow_event_ref,
    :reconciliation_status,
    :drift_class,
    :safe_operator_action,
    :release_manifest_ref
  ]
  @normalizable_keys @profile_fields ++
                       [
                         :dispatch_attempt_count,
                         :failure_class,
                         :last_dispatch_error_kind,
                         :last_dispatch_error_payload,
                         :last_reconcile_wave_id,
                         :next_dispatch_at,
                         :postgres_state,
                         :projection_state,
                         :state_ref,
                         :status,
                         :submission_ref,
                         :summary,
                         :temporal_event_or_lifecycle_ahead_of_postgres_projection,
                         :temporal_state_unknown,
                         :temporal_terminal_event_ref,
                         :temporal_terminal_status,
                         :terminal_event_ref,
                         :trace_id,
                         :workflow_id,
                         :workflow_input_version,
                         :workflow_ref,
                         :workflow_run_id,
                         :workflow_state,
                         :workflow_type,
                         :workflow_version
                       ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @row_classifications [
    %{
      table: "execution_records",
      owner: :execution_ledger,
      row_class: :fact_and_operator_projection,
      workflow_master?: false,
      repair_owner: :workflow_projection_reconciliation
    },
    %{
      table: "decision_records",
      owner: :decision_ledger,
      row_class: :decision_fact,
      workflow_master?: false,
      repair_owner: :decision_ledger
    },
    %{
      table: "audit_facts",
      owner: :audit_evidence,
      row_class: :append_only_ledger,
      workflow_master?: false,
      repair_owner: :audit_evidence
    },
    %{
      table: "workflow_start_outbox",
      owner: :workflow_runtime,
      row_class: :local_outbox_delivery_evidence,
      workflow_master?: false,
      repair_owner: :workflow_runtime
    },
    %{
      table: "lifecycle_continuations",
      owner: :owner_directed_compensation,
      row_class: :local_retry_visibility,
      workflow_master?: false,
      repair_owner: :bounded_context_owner_command_or_workflow_signal
    },
    %{
      table: "execution_lineage_records",
      owner: :audit_evidence,
      row_class: :lineage_fact,
      workflow_master?: false,
      repair_owner: :audit_evidence
    },
    %{
      table: "archival_snapshots",
      owner: :archival_evidence,
      row_class: :archival_state,
      workflow_master?: false,
      repair_owner: :archival_evidence
    }
  ]

  @candidate_query """
  WITH execution_projection AS (
    SELECT
      er.id AS execution_id,
      er.tenant_id,
      er.installation_id,
      er.subject_id,
      er.trace_id,
      er.dispatch_state AS postgres_state,
      er.last_reconcile_wave_id,
      er.updated_at AS projection_updated_at,
      (
        'tenant:' || er.tenant_id ||
        ':execution:' || er.id::text ||
        ':attempt:' || GREATEST(er.dispatch_attempt_count, 1)::text
      ) AS expected_workflow_id
    FROM execution_records er
    WHERE er.installation_id = $1
  ),
  workflow_outbox AS (
    SELECT
      wso.outbox_id,
      wso.workflow_id,
      wso.workflow_run_id,
      wso.workflow_type,
      wso.workflow_version,
      wso.workflow_input_version,
      wso.resource_ref,
      wso.dispatch_state AS outbox_state,
      wso.last_error_class,
      wso.updated_at AS outbox_updated_at
    FROM workflow_start_outbox wso
    WHERE wso.workflow_type = 'execution_attempt'
  )
  SELECT
    ep.execution_id,
    ep.tenant_id,
    ep.installation_id,
    ep.subject_id,
    ep.trace_id,
    ep.postgres_state,
    ep.last_reconcile_wave_id,
    ep.projection_updated_at,
    COALESCE(wo.workflow_id, ep.expected_workflow_id) AS workflow_id,
    wo.workflow_run_id,
    wo.workflow_type,
    wo.workflow_version,
    wo.workflow_input_version,
    wo.outbox_state,
    wo.outbox_id,
    wo.last_error_class,
    wo.outbox_updated_at
  FROM execution_projection ep
  LEFT JOIN workflow_outbox wo
    ON wo.workflow_id = ep.expected_workflow_id
    OR wo.resource_ref = ('execution:' || ep.execution_id::text)
  WHERE
    ep.postgres_state IN (
      'queued',
      'in_flight',
      'accepted_active',
      'completed',
      'cancelled',
      'failed',
      'rejected'
    )
    OR wo.outbox_id IS NOT NULL
  ORDER BY ep.projection_updated_at ASC, ep.execution_id ASC;
  """

  @drift_actions [
    %{
      drift_class: :projection_lag,
      detection: :temporal_event_or_lifecycle_ahead_of_postgres_projection,
      sla: %{first_repair_within_ms: 60_000, escalate_after_ms: 300_000},
      automatic_repair: :rebuild_projection_from_compact_workflow_query,
      operator_repair: :run_projection_repair_for_workflow_ref,
      safe_operator_action: :repair_projection
    },
    %{
      drift_class: :orphan_projection,
      detection: :postgres_projection_without_known_temporal_workflow,
      sla: %{classify_within_ms: 60_000, repair_within_ms: 300_000},
      automatic_repair: :redispatch_valid_outbox_through_workflow_runtime_idempotency,
      operator_repair: :attach_or_quarantine_projection,
      safe_operator_action: :quarantine_projection
    },
    %{
      drift_class: :orphan_workflow,
      detection: :temporal_workflow_without_expected_postgres_projection,
      sla: %{classify_within_ms: 60_000, backfill_within_ms: 300_000},
      automatic_repair: :backfill_projection_from_compact_workflow_refs,
      operator_repair: :quarantine_workflow_projection,
      safe_operator_action: :backfill_or_quarantine
    },
    %{
      drift_class: :conflicting_terminal,
      detection: :temporal_and_postgres_terminal_state_disagree,
      sla: %{quarantine_within_ms: 0, operator_review_within_ms: 900_000},
      automatic_repair: :quarantine_and_freeze_close_actions,
      operator_repair: :apply_temporal_terminal_signal_or_supersede_projection,
      safe_operator_action: :operator_review_required
    },
    %{
      drift_class: :version_skew,
      detection: :workflow_or_input_version_not_accepted_by_current_profile,
      sla: %{quarantine_within_ms: 0, accepted_version_decision_within_ms: 3_600_000},
      automatic_repair: :read_only_projection_adapter_when_accepted,
      operator_repair: :pin_old_reader_adapter_or_restart_through_version_skew_runbook,
      safe_operator_action: :read_only_quarantine
    }
  ]

  @drain_plan %{
    retryable_states: ["queued", "retryable_failure", "in_flight", "dispatching"],
    evidence_only_states: ["started", "duplicate_started"],
    invalid_row_action: :quarantine_or_dead_letter,
    start_authority: :mezzanine_workflow_runtime_idempotency,
    forbidden_worker_authority: :workflow_lifecycle_decision
  }

  @retirement_gate [
    :zero_nonterminal_rows_for_declared_window,
    :no_non_workflow_runtime_enqueue_writers_by_source_scan,
    :positive_workflow_start_and_signal_tests_preserved,
    :projection_drift_reconciliation_passes,
    :rollback_or_fail_forward_plan_recorded,
    :owner_approval_recorded
  ]

  @dispatch_state_reduction %{
    active_targets: [:queued, :in_flight, :accepted_active],
    legacy_aliases: %{},
    evidence_fields: [
      :dispatch_attempt_count,
      :last_dispatch_error_kind,
      :last_dispatch_error_payload,
      :next_dispatch_at,
      :submission_ref,
      :lower_receipt,
      :last_reconcile_wave_id
    ],
    new_legacy_writes_allowed?: false,
    drain_gate: :closed_by_m2am_strict_greenfield_source_scan,
    reader_policy: :canonical_active_states_only_after_live_rows_drain
  }

  @active_workflow_truth_policy %{
    truth_owner: :temporal,
    postgres_role: :projection_only_for_active_workflow_lifecycle,
    terminal_projection_requires: [
      :temporal_terminal_status,
      :temporal_terminal_event_ref
    ],
    forbidden_projection_actions: [
      :postgres_terminal_closes_active_workflow,
      :postgres_terminal_without_temporal_terminal_event,
      :workflow_start_outbox_dispatch_state_used_as_lifecycle_truth
    ],
    safe_operator_actions: [
      :signal_workflow,
      :cancel_workflow,
      :quarantine_projection,
      :repair_projection_from_compact_query
    ]
  }

  @spec profile() :: map()
  def profile do
    %{
      contract_name: "Mezzanine.WorkflowProjectionReconciliation.v1",
      owner_repo: :mezzanine,
      workflow_master: :temporal,
      postgres_role: :facts_ledgers_projections_local_outbox_and_archival_state,
      fields: @profile_fields,
      row_classifications: @row_classifications,
      drift_classes: Enum.map(@drift_actions, & &1.drift_class),
      candidate_query: @candidate_query,
      temporal_lookup: %{
        describe: :describe_workflow,
        query: :query_workflow,
        query_name: @query_name,
        raw_history_allowed?: false
      },
      outbox_drain_plan: @drain_plan,
      workflow_starter_retirement_gate: @retirement_gate,
      dispatch_state_reduction: @dispatch_state_reduction,
      active_workflow_truth_policy: @active_workflow_truth_policy,
      release_manifest_ref: @release_manifest_ref
    }
  end

  @spec candidate_query() :: String.t()
  def candidate_query, do: @candidate_query

  @spec postgres_row_classifications() :: [map()]
  def postgres_row_classifications, do: @row_classifications

  @spec drift_actions() :: [map()]
  def drift_actions, do: @drift_actions

  @spec outbox_drain_plan() :: map()
  def outbox_drain_plan, do: @drain_plan

  @spec workflow_starter_retirement_gate() :: [atom()]
  def workflow_starter_retirement_gate, do: @retirement_gate

  @spec dispatch_state_reduction_profile() :: map()
  def dispatch_state_reduction_profile, do: @dispatch_state_reduction

  @spec active_workflow_truth_policy() :: map()
  def active_workflow_truth_policy, do: @active_workflow_truth_policy

  @spec temporal_lookup_requests(map() | keyword()) :: [map()]
  def temporal_lookup_requests(candidate) do
    candidate = normalize(candidate)
    workflow_id = fetch_required!(candidate, :workflow_id)
    workflow_run_id = fetch_value(candidate, :workflow_run_id)

    [
      %{
        operation: :describe_workflow,
        workflow_id: workflow_id,
        run_id: workflow_run_id,
        release_manifest_ref: @release_manifest_ref
      },
      %{
        operation: :query_workflow,
        workflow_id: workflow_id,
        run_id: workflow_run_id,
        query_name: @query_name,
        release_manifest_ref: @release_manifest_ref
      }
    ]
  end

  @spec lookup_temporal_state(map() | keyword(), module()) :: {:ok, map()} | {:error, term()}
  def lookup_temporal_state(candidate, runtime \\ Mezzanine.WorkflowRuntime) do
    [describe_request, query_request] = temporal_lookup_requests(candidate)

    with {:ok, description} <- runtime.describe_workflow(describe_request),
         {:ok, query_result} <- runtime.query_workflow(query_request) do
      {:ok,
       %{
         workflow_id: describe_request.workflow_id,
         workflow_run_id: describe_request.run_id,
         description: compact_map(description),
         query: compact_map(query_result),
         query_name: @query_name,
         raw_history?: false,
         release_manifest_ref: @release_manifest_ref
       }}
    end
  end

  @spec authorize_lifecycle_projection(map() | keyword(), map() | keyword()) ::
          {:ok, map()} | {:error, map()}
  def authorize_lifecycle_projection(candidate, temporal_state) do
    candidate = normalize(candidate)
    temporal_state = normalize_nested(temporal_state)
    postgres_state = candidate |> fetch_value(:postgres_state) |> normalize_state()
    workflow_id = fetch_value(candidate, :workflow_id)
    temporal_status = temporal_state |> temporal_status() |> normalize_state()
    temporal_terminal_event_ref = temporal_terminal_event_ref(temporal_state)

    case lifecycle_projection_result(postgres_state, temporal_status, temporal_terminal_event_ref) do
      {:ok, reason, safe_action} ->
        {:ok,
         lifecycle_decision(reason, workflow_id, postgres_state, temporal_status, safe_action)}

      {:ok, reason, safe_action, terminal_event_ref} ->
        {:ok,
         reason
         |> lifecycle_decision(workflow_id, postgres_state, temporal_status, safe_action)
         |> Map.put(:terminal_event_ref, terminal_event_ref)}

      {:error, reason, safe_action} ->
        {:error,
         lifecycle_decision(reason, workflow_id, postgres_state, temporal_status, safe_action)}
    end
  end

  defp compact_map(%_{} = struct), do: struct |> Map.from_struct() |> compact_map()

  defp compact_map(value) when is_map(value) do
    Map.take(value, [
      :workflow_ref,
      :workflow_id,
      :workflow_run_id,
      :workflow_type,
      :workflow_version,
      :workflow_input_version,
      :status,
      :state_ref,
      :summary,
      :trace_id,
      :failure_class,
      :last_observed_workflow_event_ref,
      :terminal_event_ref,
      :projection_state
    ])
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize()

  defp normalize(attrs) when is_map(attrs),
    do: Map.new(attrs, fn {k, v} -> {normalize_key(k), v} end)

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)

  defp normalize_nested(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_nested()
  defp normalize_nested(%_{} = struct), do: struct |> Map.from_struct() |> normalize_nested()

  defp normalize_nested(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), normalize_nested_value(value)} end)
  end

  defp normalize_nested(other), do: other

  defp normalize_nested_value(value) when is_list(value) do
    if Keyword.keyword?(value),
      do: normalize_nested(value),
      else: Enum.map(value, &normalize_nested_value/1)
  end

  defp normalize_nested_value(value) when is_map(value), do: normalize_nested(value)
  defp normalize_nested_value(value), do: value

  defp temporal_status(temporal_state) do
    nested_get(temporal_state, [:description, :status]) ||
      nested_get(temporal_state, [:query, :summary, :workflow_state]) ||
      nested_get(temporal_state, [:query, :projection_state]) ||
      nested_get(temporal_state, [:workflow_state]) ||
      nested_get(temporal_state, [:status])
  end

  defp temporal_terminal_event_ref(temporal_state) do
    nested_get(temporal_state, [:query, :summary, :terminal_event_ref]) ||
      nested_get(temporal_state, [:query, :terminal_event_ref]) ||
      nested_get(temporal_state, [:terminal_event_ref])
  end

  defp nested_get(attrs, keys) do
    Enum.reduce_while(keys, attrs, fn key, acc ->
      case fetch_value(acc, key) do
        nil -> {:halt, nil}
        value -> {:cont, value}
      end
    end)
  end

  defp lifecycle_decision(reason, workflow_id, postgres_state, temporal_status, safe_action) do
    %{
      workflow_truth_owner: :temporal,
      postgres_role: :projection_only,
      reason: reason,
      workflow_id: workflow_id,
      postgres_state: postgres_state,
      temporal_status: temporal_status,
      safe_operator_action: safe_action,
      release_manifest_ref: @release_manifest_ref
    }
  end

  defp lifecycle_projection_result(postgres_state, _temporal_status, _terminal_event_ref)
       when postgres_state in @postgres_active_states do
    {:ok, :active_projection, :projection_only}
  end

  defp lifecycle_projection_result(postgres_state, temporal_status, _terminal_event_ref)
       when postgres_state in @postgres_terminal_states and
              temporal_status in @temporal_active_states do
    {:error, :postgres_terminal_closes_active_workflow, :signal_or_quarantine}
  end

  defp lifecycle_projection_result(postgres_state, temporal_status, terminal_event_ref)
       when postgres_state in @postgres_terminal_states and
              temporal_status in @temporal_terminal_states do
    if present?(terminal_event_ref) do
      {:ok, :temporal_terminal_projection, :project_temporal_terminal, terminal_event_ref}
    else
      {:error, :missing_temporal_terminal_event_ref, :quarantine_projection}
    end
  end

  defp lifecycle_projection_result(postgres_state, _temporal_status, _terminal_event_ref)
       when postgres_state in @postgres_terminal_states do
    {:error, :temporal_state_unknown, :describe_and_query_workflow}
  end

  defp lifecycle_projection_result(_postgres_state, _temporal_status, _terminal_event_ref) do
    {:error, :unsupported_postgres_lifecycle_state, :quarantine_projection}
  end

  defp normalize_state(value) when is_atom(value),
    do: value |> Atom.to_string() |> normalize_state()

  defp normalize_state(value) when is_binary(value), do: value |> String.downcase()
  defp normalize_state(_value), do: nil

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)

  defp fetch_required!(attrs, key) do
    case fetch_value(attrs, key) do
      value when is_binary(value) and value != "" -> value
      value when not is_nil(value) -> value
      _ -> raise ArgumentError, "missing required reconciliation candidate field #{key}"
    end
  end

  defp fetch_value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
end
