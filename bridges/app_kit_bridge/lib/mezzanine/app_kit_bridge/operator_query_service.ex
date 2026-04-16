defmodule Mezzanine.AppKitBridge.OperatorQueryService do
  @moduledoc """
  Backend-oriented operator projections, health reads, and unified trace access.

  This service returns adapter-shaped maps so widened northbound contracts can
  bind here without inheriting the deprecated operator-surface structs.
  """

  require Ash.Query

  alias AppKit.Core.RunRef
  alias Ecto.Adapters.SQL
  alias Mezzanine.AppKitBridge.AdapterSupport
  alias Mezzanine.AppKitBridge.ReviewQueryService
  alias Mezzanine.AppKitBridge.WorkQueryService
  alias Mezzanine.Audit.ExecutionLineage
  alias Mezzanine.Audit.Repo
  alias Mezzanine.Audit.UnifiedTrace
  alias Mezzanine.Audit.UnifiedTrace.Query
  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Decisions.DecisionRecord
  alias Mezzanine.EvidenceLedger.EvidenceRecord
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.IntegrationBridge
  alias Mezzanine.Runs.RunSeries
  alias Mezzanine.Work.WorkObject
  alias MezzanineOpsModel.Intent.ReadIntent

  @default_lower_operations [:fetch_run, :events, :attempts, :run_artifacts]

  @spec run_status(RunRef.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  def run_status(%RunRef{} = run_ref, attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    with {:ok, tenant_id} <- fetch_tenant_id(run_ref, attrs, opts),
         {:ok, work_object_id} <- fetch_work_object_id(run_ref, attrs),
         {:ok, subject_status} <- subject_status(tenant_id, work_object_id) do
      payload = Map.get(subject_status, :payload, %{})

      {:ok,
       %{
         run_ref: run_ref,
         work_object_id: work_object_id,
         timeline: Map.get(payload, :timeline, []),
         audit_events: Map.get(payload, :audit_events, []),
         evidence_bundles: Map.get(payload, :evidence_bundles, []),
         gate_status: Map.get(payload, :gate_status, %{})
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason)}
    end
  end

  @spec subject_status(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def subject_status(tenant_id, subject_id)
      when is_binary(tenant_id) and is_binary(subject_id) do
    with {:ok, detail} <- WorkQueryService.get_subject_detail(tenant_id, subject_id),
         {:ok, audit_report} <- Mezzanine.Audit.work_report(tenant_id, subject_id) do
      subject_ref = subject_ref(subject_id)

      {:ok,
       %{
         subject_ref: subject_ref,
         lifecycle_state: normalize_state(detail.status),
         title: detail.title,
         description: detail.description,
         current_execution_ref: current_execution_ref(detail, subject_ref),
         pending_decision_refs:
           Enum.map(detail.pending_review_ids, &decision_ref(&1, subject_ref, :operator_review)),
         available_actions: available_actions_from_detail(detail, subject_ref),
         payload: %{
           program_id: detail.program_id,
           work_class_id: detail.work_class_id,
           external_ref: detail.external_ref,
           priority: detail.priority,
           source_kind: detail.source_kind,
           current_plan_id: detail.current_plan_id,
           current_plan_status: normalize_state(detail.current_plan_status),
           active_run_id: detail.active_run_id,
           active_run_status: normalize_state(detail.active_run_status),
           control_session_id: detail.control_session_id,
           control_mode: normalize_state(detail.control_mode),
           gate_status: normalize_value(detail.gate_status),
           timeline: Enum.map(audit_report.timeline, &timeline_entry/1),
           audit_events: Enum.map(audit_report.audit_events, &normalize_value/1),
           evidence_bundles: normalize_value(audit_report.evidence_bundles),
           evidence_bundle_id: detail.evidence_bundle_id,
           run_series_ids: detail.run_series_ids,
           obligation_ids: detail.obligation_ids,
           last_event_at: detail.last_event_at
         }
       }}
    end
  end

  @spec timeline(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def timeline(tenant_id, subject_id) when is_binary(tenant_id) and is_binary(subject_id) do
    with {:ok, detail} <- WorkQueryService.get_subject_detail(tenant_id, subject_id) do
      payload = Map.get(detail, :timeline, [])

      {:ok,
       %{
         subject_ref: subject_ref(subject_id),
         entries: Enum.map(payload, &timeline_entry/1),
         last_event_at: detail.last_event_at
       }}
    end
  end

  @spec available_actions(String.t(), Ecto.UUID.t()) :: {:ok, [map()]} | {:error, term()}
  def available_actions(tenant_id, subject_id)
      when is_binary(tenant_id) and is_binary(subject_id) do
    with {:ok, detail} <- WorkQueryService.get_subject_detail(tenant_id, subject_id) do
      {:ok, available_actions_from_detail(detail, subject_ref(subject_id))}
    end
  end

  @spec list_operator_alerts(String.t(), Ecto.UUID.t()) :: {:ok, [map()]} | {:error, term()}
  def list_operator_alerts(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, subjects} <- WorkQueryService.list_subjects(tenant_id, program_id, %{}) do
      alerts =
        subjects
        |> Enum.flat_map(&alerts_for_subject_summary(tenant_id, &1))
        |> Enum.sort_by(&alert_sort_key/1, :desc)

      {:ok, alerts}
    end
  end

  @spec list_pending_reviews(String.t(), Ecto.UUID.t()) :: {:ok, [map()]} | {:error, term()}
  def list_pending_reviews(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    ReviewQueryService.list_pending_reviews(tenant_id, program_id)
  end

  @spec system_health(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def system_health(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, queue_stats} <- WorkQueryService.queue_stats(tenant_id, program_id),
         {:ok, pending_reviews} <- list_pending_reviews(tenant_id, program_id),
         {:ok, open_control_sessions} <- open_control_sessions(tenant_id, program_id),
         {:ok, active_run_count} <- active_run_count(tenant_id, program_id) do
      {:ok,
       %{
         program_id: program_id,
         queue_stats: queue_stats,
         pending_review_count: length(pending_reviews),
         open_control_session_count: length(open_control_sessions),
         active_run_count: active_run_count,
         stalled_run_count: Map.get(queue_stats, :stalled_count, 0),
         open_escalation_count: Map.get(queue_stats, :open_escalation_count, 0)
       }}
    end
  end

  @spec get_unified_trace(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def get_unified_trace(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    attrs = Map.new(attrs)

    with {:ok, installation_id} <- fetch_string(attrs, opts, :installation_id),
         {:ok, execution_id} <- fetch_string(attrs, opts, :execution_id),
         {:ok, trace_id} <- fetch_string(attrs, opts, :trace_id),
         :ok <- authorize_trace_query(installation_id, execution_id, trace_id),
         {:ok, query} <- build_unified_trace_query(attrs, installation_id, trace_id),
         {:ok, sources} <- fetch_trace_sources(attrs, opts, query, execution_id),
         {:ok, timeline} <- UnifiedTrace.assemble(query, sources) do
      {:ok,
       %{
         trace_id: timeline.trace_id,
         installation_id: timeline.installation_id,
         join_keys: trace_join_keys(attrs, timeline, execution_id),
         metadata: %{
           indexed_join_keys: Enum.map(timeline.join_keys, &Atom.to_string/1)
         },
         steps: Enum.map(timeline.steps, &normalize_unified_step/1)
       }}
    end
  rescue
    ArgumentError -> {:error, :invalid_trace_query}
  end

  defp build_unified_trace_query(attrs, installation_id, trace_id) do
    query =
      Query.new!(%{
        trace_id: trace_id,
        installation_id: installation_id,
        include_lower?: Map.get(attrs, :include_lower?, true),
        include_diagnostic?: Map.get(attrs, :include_diagnostic?, false)
      })

    {:ok, query}
  end

  defp fetch_trace_sources(attrs, opts, %Query{} = query, execution_id) do
    with {:ok, audit_facts} <- list_audit_facts(query.installation_id, query.trace_id),
         {:ok, executions} <- list_execution_records(query.installation_id, query.trace_id),
         {:ok, decisions} <- list_decision_records(query.installation_id, query.trace_id),
         {:ok, evidence} <- list_evidence_records(query.installation_id, query.trace_id),
         {:ok, lower_facts} <- lower_trace_facts(attrs, opts, query, execution_id) do
      {:ok,
       %{
         audit_facts: audit_facts,
         executions: executions,
         decisions: decisions,
         evidence: evidence,
         lower_facts: lower_facts
       }}
    end
  end

  defp list_audit_facts(installation_id, trace_id) do
    with {:ok, result} <-
           SQL.query(
             Repo,
             """
             SELECT id, trace_id, causation_id, occurred_at, fact_kind, actor_ref, payload
             FROM audit_facts
             WHERE installation_id = $1 AND trace_id = $2
             ORDER BY occurred_at ASC
             """,
             [installation_id, trace_id]
           ) do
      {:ok,
       Enum.map(result_rows(result), fn row ->
         %{
           id: row.id,
           trace_id: row.trace_id,
           causation_id: row.causation_id,
           occurred_at: coerce_datetime(row.occurred_at),
           fact_kind: row.fact_kind,
           actor_ref: normalize_value(row.actor_ref),
           payload: normalize_value(row.payload)
         }
       end)}
    end
  end

  defp list_execution_records(installation_id, trace_id) do
    ExecutionRecord
    |> Ash.Query.filter(installation_id == ^installation_id and trace_id == ^trace_id)
    |> Ash.read(authorize?: false, domain: Mezzanine.Execution)
    |> case do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn row ->
           %{
             id: row.id,
             trace_id: row.trace_id,
             causation_id: row.causation_id,
             occurred_at: coerce_datetime(row.updated_at || row.inserted_at),
             subject_id: row.subject_id,
             dispatch_state: row.dispatch_state,
             recipe_ref: row.recipe_ref,
             compiled_pack_revision: row.compiled_pack_revision,
             failure_kind: row.failure_kind,
             terminal_rejection_reason: row.terminal_rejection_reason
           }
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp authorize_trace_query(installation_id, execution_id, trace_id) do
    with {:ok, execution} <- fetch_execution_record(execution_id) do
      cond do
        is_nil(execution) ->
          :ok

        execution.installation_id != installation_id ->
          {:error, :unauthorized_lower_read}

        execution.trace_id != trace_id ->
          {:error, :invalid_trace_query}

        true ->
          :ok
      end
    end
  end

  defp fetch_execution_record(execution_id) do
    ExecutionRecord
    |> Ash.Query.filter(id == ^execution_id)
    |> Ash.read_one(authorize?: false, domain: Mezzanine.Execution)
  end

  defp list_decision_records(installation_id, trace_id) do
    DecisionRecord
    |> Ash.Query.filter(installation_id == ^installation_id and trace_id == ^trace_id)
    |> Ash.read(authorize?: false, domain: Mezzanine.Decisions)
    |> case do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn row ->
           %{
             id: row.id,
             trace_id: row.trace_id,
             causation_id: row.causation_id,
             occurred_at: coerce_datetime(row.updated_at || row.inserted_at),
             subject_id: row.subject_id,
             execution_id: row.execution_id,
             decision_kind: row.decision_kind,
             lifecycle_state: row.lifecycle_state,
             decision_value: row.decision_value,
             reason: row.reason
           }
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp list_evidence_records(installation_id, trace_id) do
    EvidenceRecord
    |> Ash.Query.filter(installation_id == ^installation_id and trace_id == ^trace_id)
    |> Ash.read(authorize?: false, domain: Mezzanine.EvidenceLedger)
    |> case do
      {:ok, rows} ->
        {:ok,
         Enum.map(rows, fn row ->
           %{
             id: row.id,
             trace_id: row.trace_id,
             causation_id: row.causation_id,
             occurred_at: coerce_datetime(row.updated_at || row.inserted_at),
             subject_id: row.subject_id,
             execution_id: row.execution_id,
             evidence_kind: row.evidence_kind,
             status: row.status,
             collector_ref: row.collector_ref,
             content_ref: row.content_ref,
             metadata: normalize_value(row.metadata)
           }
         end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp lower_trace_facts(_attrs, _opts, %Query{include_lower?: false}, _execution_id),
    do: {:ok, []}

  defp lower_trace_facts(attrs, opts, %Query{} = query, execution_id) do
    operations = Keyword.get(opts, :lower_operations, @default_lower_operations)
    now = Keyword.get(opts, :now, DateTime.utc_now())

    Enum.reduce_while(operations, {:ok, []}, fn operation, {:ok, acc} ->
      accumulate_lower_fact(operation, acc, attrs, opts, query, execution_id, now)
    end)
  end

  defp dispatch_lower_fact(operation, attrs, opts, installation_id, execution_id, now) do
    read_intent =
      ReadIntent.new!(%{
        intent_id: "operator-trace:#{execution_id}:#{operation}",
        read_type: :lower_fact,
        subject: %{
          actor_id: Map.get(attrs, :actor_id, "operator"),
          installation_id: installation_id,
          execution_id: execution_id
        },
        query: lower_query(operation, attrs)
      })

    lower_opts =
      case Keyword.fetch(opts, :lower_facts) do
        {:ok, lower_facts} -> [lower_facts: lower_facts, fetch_lineage: &fetch_lineage/1]
        :error -> [fetch_lineage: &fetch_lineage/1]
      end

    with {:ok, response} <- IntegrationBridge.dispatch_read(read_intent, lower_opts) do
      {:ok,
       normalize_lower_records(response, execution_id, now, attrs.trace_id || attrs["trace_id"])}
    end
  end

  defp lower_query(:fetch_attempt, attrs) do
    %{operation: :fetch_attempt, attempt_id: Map.get(attrs, :attempt_id)}
  end

  defp lower_query(:fetch_artifact, attrs) do
    %{operation: :fetch_artifact, artifact_id: Map.get(attrs, :artifact_id)}
  end

  defp lower_query(operation, _attrs), do: %{operation: operation}

  defp normalize_lower_records(
         %{operation: operation, source: source, result: result},
         execution_id,
         now,
         trace_id
       ) do
    result
    |> List.wrap()
    |> Enum.with_index()
    |> Enum.map(fn {item, index} ->
      normalize_lower_record(operation, source, item, execution_id, index, now, trace_id)
    end)
  end

  defp normalize_lower_record(operation, source, item, execution_id, index, now, trace_id) do
    item = normalize_value(item)

    item
    |> Map.put(:id, lower_record_id(item, operation, index))
    |> Map.put(:trace_id, Map.get(item, :trace_id) || trace_id)
    |> Map.put_new(:causation_id, Map.get(item, :causation_id, execution_id))
    |> Map.put_new(:occurred_at, coerce_datetime(lower_occurred_at(item, now)))
    |> Map.put(:source, source)
  end

  defp diagnostic_lower_fact(trace_id, execution_id, operation, reason, occurred_at) do
    %{
      id: "diagnostic:#{operation}",
      trace_id: trace_id,
      causation_id: execution_id,
      occurred_at: occurred_at,
      source: :bridge_diagnostic,
      operation: operation,
      error: inspect(reason)
    }
  end

  defp lower_record_id(item, operation, index) when is_map(item) do
    Map.get(item, :id) || Map.get(item, :event_id) || Map.get(item, :attempt_id) ||
      Map.get(item, :artifact_id) || Map.get(item, :run_id) || "#{operation}:#{index}"
  end

  defp lower_record_id(_item, operation, index), do: "#{operation}:#{index}"

  defp lower_occurred_at(item, fallback) do
    Map.get(item, :occurred_at) || Map.get(item, :updated_at) || Map.get(item, :inserted_at) ||
      fallback
  end

  defp fetch_lineage(execution_id) do
    with {:ok, result} <-
           SQL.query(
             Repo,
             """
             SELECT trace_id, causation_id, installation_id, subject_id, execution_id,
                    dispatch_outbox_entry_id, citadel_request_id, citadel_submission_id,
                    ji_submission_key, lower_run_id, lower_attempt_id, artifact_refs
             FROM execution_lineage_records
             WHERE execution_id = $1
             LIMIT 1
             """,
             [execution_id]
           ),
         [row | _] <- result_rows(result) do
      {:ok,
       ExecutionLineage.new!(%{
         trace_id: row.trace_id,
         causation_id: row.causation_id,
         installation_id: row.installation_id,
         subject_id: row.subject_id,
         execution_id: row.execution_id,
         dispatch_outbox_entry_id: row.dispatch_outbox_entry_id,
         citadel_request_id: row.citadel_request_id,
         citadel_submission_id: row.citadel_submission_id,
         ji_submission_key: row.ji_submission_key,
         lower_run_id: row.lower_run_id,
         lower_attempt_id: row.lower_attempt_id,
         artifact_refs: row.artifact_refs || []
       })}
    else
      [] -> {:error, :unknown_execution_lineage}
      {:error, reason} -> {:error, reason}
    end
  end

  defp result_rows(%Postgrex.Result{columns: columns, rows: rows}) do
    Enum.map(rows, fn row ->
      Enum.zip(columns, row) |> Map.new(fn {key, value} -> {String.to_atom(key), value} end)
    end)
  end

  defp open_control_sessions(tenant_id, program_id) do
    ControlSession.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, sessions} -> {:ok, Enum.filter(sessions, &(&1.status == :active))}
      {:error, reason} -> {:error, reason}
    end
  end

  defp active_run_count(tenant_id, program_id) do
    with {:ok, work_objects} <-
           WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      work_objects
      |> Enum.map(& &1.id)
      |> count_active_runs_for_work_ids(tenant_id)
    end
  end

  defp alerts_for_subject_summary(tenant_id, subject) do
    case WorkQueryService.get_subject_detail(tenant_id, subject.subject_id) do
      {:ok, detail} -> alerts_for_subject(detail)
      {:error, _reason} -> []
    end
  end

  defp alerts_for_subject(detail) do
    raised_at = coerce_datetime(detail.last_event_at) || DateTime.utc_now()
    ref = subject_ref(detail.subject_id)

    []
    |> maybe_add_alert(detail.status == :blocked, %{
      subject_ref: ref,
      alert_kind: :blocked,
      severity: :warning,
      message: "Work is blocked and needs operator attention",
      raised_at: raised_at,
      title: detail.title
    })
    |> maybe_add_alert(Map.get(detail.gate_status, :escalated_count, 0) > 0, %{
      subject_ref: ref,
      alert_kind: :escalated_review,
      severity: :critical,
      message: "Review escalation is open",
      raised_at: raised_at,
      title: detail.title
    })
    |> maybe_add_alert(Map.get(detail.gate_status, :pending_count, 0) > 0, %{
      subject_ref: ref,
      alert_kind: :pending_review,
      severity: :info,
      message: "Review is pending before release",
      raised_at: raised_at,
      title: detail.title
    })
    |> maybe_add_alert(detail.active_run_status == :stalled, %{
      subject_ref: ref,
      alert_kind: :stalled,
      severity: :critical,
      message: "Active run is stalled",
      raised_at: raised_at,
      title: detail.title
    })
  end

  defp maybe_add_alert(alerts, true, alert), do: [alert | alerts]
  defp maybe_add_alert(alerts, false, _alert), do: alerts

  defp accumulate_lower_fact(operation, acc, attrs, opts, query, execution_id, now) do
    operation
    |> dispatch_lower_fact(attrs, opts, query.installation_id, execution_id, now)
    |> merge_lower_fact_result(acc, query, execution_id, operation, now)
  end

  defp merge_lower_fact_result({:ok, records}, acc, _query, _execution_id, _operation, _now) do
    {:cont, {:ok, acc ++ records}}
  end

  defp merge_lower_fact_result(
         {:error, :unauthorized_lower_read},
         _acc,
         _query,
         _execution_id,
         _operation,
         _now
       ) do
    {:halt, {:error, :unauthorized_lower_read}}
  end

  defp merge_lower_fact_result({:error, reason}, acc, query, execution_id, operation, now) do
    {:cont, {:ok, acc ++ diagnostic_lower_facts(query, execution_id, operation, reason, now)}}
  end

  defp diagnostic_lower_facts(
         %Query{include_diagnostic?: true} = query,
         execution_id,
         operation,
         reason,
         now
       ) do
    [diagnostic_lower_fact(query.trace_id, execution_id, operation, reason, now)]
  end

  defp diagnostic_lower_facts(%Query{}, _execution_id, _operation, _reason, _now), do: []

  defp alert_sort_key(alert) do
    raised_at = coerce_datetime(alert.raised_at) || DateTime.utc_now()
    {severity_rank(alert.severity), DateTime.to_unix(raised_at, :microsecond)}
  end

  defp count_active_runs_for_work_ids([], _tenant_id), do: {:ok, 0}

  defp count_active_runs_for_work_ids(work_ids, tenant_id) do
    RunSeries
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id in ^work_ids and status == :active)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
    |> case do
      {:ok, series} -> {:ok, Enum.count(series, &is_binary(&1.current_run_id))}
      {:error, reason} -> {:error, reason}
    end
  end

  defp available_actions_from_detail(detail, subject_ref) do
    pause_or_resume =
      if detail.control_mode == :paused do
        [:resume]
      else
        [:pause]
      end

    (pause_or_resume ++ [:cancel, :replan, :grant_override])
    |> Enum.uniq()
    |> Enum.map(&action_ref(&1, subject_ref))
  end

  defp current_execution_ref(%{active_run_id: nil}, _subject_ref), do: nil

  defp current_execution_ref(detail, subject_ref) do
    %{
      id: detail.active_run_id,
      subject_ref: subject_ref,
      dispatch_state: normalize_state(detail.active_run_status)
    }
  end

  defp timeline_entry(entry) do
    entry
    |> normalize_value()
    |> Map.update(:event_kind, nil, &normalize_state/1)
  end

  defp normalize_unified_step(step) do
    %{
      ref: step.ref,
      source: step.source,
      occurred_at: step.occurred_at,
      trace_id: step.trace_id,
      causation_id: step.causation_id,
      freshness: step.freshness,
      operator_actionable?: step.operator_actionable?,
      diagnostic?: step.diagnostic?,
      payload: normalize_value(step.payload)
    }
  end

  defp trace_join_keys(attrs, timeline, execution_id) do
    attrs
    |> Map.new()
    |> Map.take([:subject_id, "subject_id", :causation_id, "causation_id"])
    |> Enum.reduce(
      %{
        "trace_id" => timeline.trace_id,
        "installation_id" => timeline.installation_id,
        "execution_id" => execution_id
      },
      fn
        {:subject_id, value}, acc when is_binary(value) -> Map.put(acc, "subject_id", value)
        {"subject_id", value}, acc when is_binary(value) -> Map.put(acc, "subject_id", value)
        {:causation_id, value}, acc when is_binary(value) -> Map.put(acc, "causation_id", value)
        {"causation_id", value}, acc when is_binary(value) -> Map.put(acc, "causation_id", value)
        {_key, _value}, acc -> acc
      end
    )
  end

  defp subject_ref(subject_id), do: %{id: subject_id, subject_kind: "work_object"}

  defp decision_ref(review_unit_id, subject_ref, review_kind) do
    %{
      id: review_unit_id,
      decision_kind: normalize_state(review_kind),
      subject_ref: subject_ref
    }
  end

  defp action_ref(action_kind, subject_ref) do
    action_kind = normalize_state(action_kind)

    %{
      id: "#{subject_ref.id}:#{action_kind}",
      action_kind: action_kind,
      subject_ref: subject_ref
    }
  end

  defp fetch_tenant_id(run_ref, attrs, opts) do
    case Keyword.get(opts, :tenant_id) || Map.get(attrs, :tenant_id) ||
           Map.get(attrs, "tenant_id") ||
           Map.get(run_ref.metadata, :tenant_id) || Map.get(run_ref.metadata, "tenant_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_tenant_id}
    end
  end

  defp fetch_work_object_id(run_ref, attrs) do
    case Map.get(attrs, :work_object_id) || Map.get(attrs, "work_object_id") ||
           Map.get(run_ref.metadata, :work_object_id) ||
           Map.get(run_ref.metadata, "work_object_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :missing_work_object_id}
    end
  end

  defp fetch_string(attrs, opts, key), do: AdapterSupport.fetch_string(attrs, opts, key)
  defp actor(tenant_id), do: AdapterSupport.actor(tenant_id)
  defp normalize_state(value), do: AdapterSupport.normalize_state(value)

  defp coerce_datetime(%DateTime{} = value), do: value
  defp coerce_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")

  defp coerce_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        datetime

      _ ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive_datetime} -> DateTime.from_naive!(naive_datetime, "Etc/UTC")
          _ -> nil
        end
    end
  end

  defp coerce_datetime(_value), do: nil

  defp normalize_value(value), do: AdapterSupport.normalize_value(value)

  defp severity_rank(:critical), do: 3
  defp severity_rank(:warning), do: 2
  defp severity_rank(:info), do: 1

  defp normalize_error(reason), do: AdapterSupport.normalize_error(reason)
end
