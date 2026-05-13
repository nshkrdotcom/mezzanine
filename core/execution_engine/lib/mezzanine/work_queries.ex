defmodule Mezzanine.WorkQueries do
  @moduledoc """
  Neutral governed-work reads and intake helpers used by the northbound bridge.
  """

  require Ash.Query

  alias Mezzanine.Audit.WorkAudit
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Review.{Escalation, ReviewUnit}
  alias Mezzanine.Reviews
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.ServiceSupport
  alias Mezzanine.Work.WorkObject
  alias Mezzanine.Work.WorkPlan
  alias Mezzanine.WorkControl
  alias Mezzanine.WorkProjectionFacts

  @active_statuses [:pending, :planning, :planned, :running, :awaiting_review, :blocked]
  @running_projection_state "running"
  @runtime_projection_name "operator_subject_runtime"

  @spec ingest_subject(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def ingest_subject(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    attrs = Map.new(attrs)

    with {:ok, tenant_id} <- fetch_string(attrs, opts, :tenant_id),
         {:ok, program_id} <- fetch_string(attrs, opts, :program_id),
         {:ok, work_class_id} <- fetch_string(attrs, opts, :work_class_id),
         {:ok, external_ref} <- fetch_string(attrs, opts, :external_ref),
         {:ok, work_object} <-
           upsert_work_object(tenant_id, program_id, work_class_id, external_ref, attrs),
         {:ok, planned_work_object} <- refresh_plan(work_object, tenant_id, attrs) do
      {:ok, subject_summary(planned_work_object)}
    end
  end

  @spec list_subjects(String.t(), Ecto.UUID.t(), map()) :: {:ok, [map()]} | {:error, term()}
  def list_subjects(tenant_id, program_id, filters \\ %{})
      when is_binary(tenant_id) and is_binary(program_id) and is_map(filters) do
    with {:ok, work_objects} <-
           WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok,
       work_objects
       |> Enum.filter(&active_work_object?(&1, filters))
       |> Enum.map(&subject_summary/1)}
    end
  end

  @spec get_subject_detail(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def get_subject_detail(tenant_id, subject_id)
      when is_binary(tenant_id) and is_binary(subject_id) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, subject_id),
         {:ok, current_plan} <- fetch_current_plan(tenant_id, work_object.current_plan_id),
         {:ok, run_series} <- list_run_series(tenant_id, work_object.id),
         {:ok, active_run} <- fetch_active_run(tenant_id, run_series),
         {:ok, active_execution} <- fetch_active_execution(work_object.id),
         {:ok, latest_execution} <- fetch_latest_execution(work_object.id),
         {:ok, pending_reviews} <- list_pending_reviews_for_work(tenant_id, work_object.id),
         {:ok, control_session} <- fetch_control_session(tenant_id, work_object.id),
         {:ok, audit_report} <- WorkAudit.work_report(tenant_id, work_object.id),
         {:ok, gate_status} <- Reviews.gate_status(tenant_id, work_object.id) do
      projection_facts =
        WorkProjectionFacts.build(
          work_object,
          current_plan,
          active_run,
          pending_reviews,
          control_session,
          gate_status
        )

      {:ok,
       %{
         subject_id: work_object.id,
         subject_kind: :work_object,
         program_id: work_object.program_id,
         work_class_id: work_object.work_class_id,
         external_ref: work_object.external_ref,
         title: work_object.title,
         description: work_object.description,
         status: work_object.status,
         priority: work_object.priority,
         source_kind: work_object.source_kind,
         source_payload: source_payload(work_object),
         current_plan_id: current_plan_id(current_plan),
         current_plan_status: current_plan_status(current_plan),
         active_run_id: active_run_id(active_run),
         active_run_status: active_run_status(active_run),
         active_execution_id: active_execution_id(active_execution),
         active_execution_dispatch_state: active_execution_dispatch_state(active_execution),
         active_execution_trace_id: active_execution_trace_id(active_execution),
         latest_execution_id: latest_execution_id(latest_execution),
         latest_execution_dispatch_state: latest_execution_dispatch_state(latest_execution),
         latest_execution_trace_id: latest_execution_trace_id(latest_execution),
         run_series_ids: Enum.map(run_series, & &1.id),
         obligation_ids: obligation_ids(current_plan),
         pending_review_ids: Enum.map(pending_reviews, & &1.id),
         evidence_bundle_id: latest_evidence_bundle_id(audit_report),
         control_session_id: control_session_id(control_session),
         control_mode: control_mode(control_session),
         gate_status: gate_status,
         pending_obligations: projection_facts.pending_obligations,
         blocking_conditions: projection_facts.blocking_conditions,
         next_step_preview: projection_facts.next_step_preview,
         timeline: audit_report.timeline,
         audit_events: audit_report.audit_events,
         last_event_at: last_event_at(audit_report.timeline)
       }}
    end
  end

  @spec get_subject_projection(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def get_subject_projection(tenant_id, subject_id)
      when is_binary(tenant_id) and is_binary(subject_id) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, subject_id),
         {:ok, current_plan} <- fetch_current_plan(tenant_id, work_object.current_plan_id),
         {:ok, run_series} <- list_run_series(tenant_id, work_object.id),
         {:ok, active_run} <- fetch_active_run(tenant_id, run_series),
         {:ok, active_execution} <- fetch_active_execution(work_object.id),
         {:ok, latest_execution} <- fetch_latest_execution(work_object.id),
         {:ok, pending_reviews} <- list_pending_reviews_for_work(tenant_id, work_object.id),
         {:ok, control_session} <- fetch_control_session(tenant_id, work_object.id),
         {:ok, gate_status} <- Reviews.gate_status(tenant_id, work_object.id),
         {:ok, timeline_projection} <- WorkAudit.timeline_for_work(tenant_id, work_object.id) do
      projection_facts =
        WorkProjectionFacts.build(
          work_object,
          current_plan,
          active_run,
          pending_reviews,
          control_session,
          gate_status
        )

      {:ok,
       %{
         subject_id: work_object.id,
         subject_kind: :work_object,
         work_status: work_object.status,
         plan_status: current_plan_status(current_plan),
         run_status: active_run_status(active_run),
         execution_dispatch_state: active_execution_dispatch_state(active_execution),
         latest_execution_dispatch_state: latest_execution_dispatch_state(latest_execution),
         latest_execution_trace_id: latest_execution_trace_id(latest_execution),
         control_mode: control_mode(control_session),
         review_status: gate_status.status,
         release_ready?: gate_status.release_ready?,
         pending_obligations: projection_facts.pending_obligations,
         blocking_conditions: projection_facts.blocking_conditions,
         next_step_preview: projection_facts.next_step_preview,
         last_event_at: timeline_projection.last_event_at
       }}
    end
  end

  @spec get_subject_runtime_projection(String.t(), Ecto.UUID.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def get_subject_runtime_projection(tenant_id, subject_id, opts \\ [])
      when is_binary(tenant_id) and is_binary(subject_id) and is_list(opts) do
    with {:ok, work_object} <- fetch_work_object(tenant_id, subject_id),
         {:ok, current_plan} <- fetch_current_plan(tenant_id, work_object.current_plan_id),
         {:ok, run_series} <- list_run_series(tenant_id, work_object.id),
         {:ok, active_run} <- fetch_active_run(tenant_id, run_series),
         {:ok, active_execution} <- fetch_active_execution(work_object.id),
         {:ok, latest_execution} <- fetch_latest_execution(work_object.id),
         {:ok, execution} <- runtime_execution(active_execution, latest_execution),
         {:ok, pending_reviews} <- list_pending_reviews_for_work(tenant_id, work_object.id),
         {:ok, control_session} <- fetch_control_session(tenant_id, work_object.id),
         {:ok, gate_status} <- Reviews.gate_status(tenant_id, work_object.id) do
      projection_facts =
        WorkProjectionFacts.build(
          work_object,
          current_plan,
          active_run,
          pending_reviews,
          control_session,
          gate_status
        )

      {:ok,
       runtime_projection(%{
         tenant_id: tenant_id,
         work_object: work_object,
         current_plan: current_plan,
         active_run: active_run,
         execution: execution,
         pending_reviews: pending_reviews,
         gate_status: gate_status,
         projection_facts: projection_facts,
         opts: opts
       })}
    else
      {:error, :not_found} -> {:error, :runtime_projection_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec queue_stats(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def queue_stats(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_objects} <- list_subjects(tenant_id, program_id),
         {:ok, open_escalation_count} <- open_escalation_count(tenant_id, work_objects),
         {:ok, stalled_count} <- stalled_count(tenant_id, work_objects) do
      counts_by_status = Enum.frequencies_by(work_objects, & &1.status)

      {:ok,
       %{
         program_id: program_id,
         active_count: length(work_objects),
         queued_count:
           Map.get(counts_by_status, :pending, 0) + Map.get(counts_by_status, :planned, 0),
         running_count: Map.get(counts_by_status, :running, 0),
         awaiting_review_count: Map.get(counts_by_status, :awaiting_review, 0),
         blocked_count: Map.get(counts_by_status, :blocked, 0),
         stalled_count: stalled_count,
         open_escalation_count: open_escalation_count,
         counts_by_status: counts_by_status
       }}
    end
  end

  @spec active_run_count(String.t(), Ecto.UUID.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def active_run_count(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_objects} <-
           WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      work_objects
      |> Enum.map(& &1.id)
      |> count_active_runs_for_work_ids(tenant_id)
    end
  end

  defp subject_summary(work_object) do
    %{
      subject_id: work_object.id,
      subject_kind: :work_object,
      program_id: work_object.program_id,
      work_class_id: work_object.work_class_id,
      external_ref: work_object.external_ref,
      title: work_object.title,
      description: work_object.description,
      status: work_object.status,
      priority: work_object.priority,
      source_kind: work_object.source_kind,
      source_payload: source_payload(work_object),
      current_plan_id: work_object.current_plan_id,
      inserted_at: work_object.inserted_at,
      updated_at: work_object.updated_at
    }
  end

  defp upsert_work_object(tenant_id, program_id, work_class_id, external_ref, attrs) do
    case find_work_object_by_external_ref(tenant_id, program_id, external_ref) do
      {:ok, nil} ->
        WorkObject.ingest(
          intake_attrs(attrs, program_id, work_class_id, external_ref),
          actor: actor(tenant_id),
          tenant: tenant_id
        )

      {:ok, %WorkObject{} = work_object} ->
        work_object
        |> Ash.Changeset.for_update(
          :refresh_intake,
          refresh_intake_attrs(attrs, work_class_id, external_ref)
        )
        |> Ash.Changeset.set_tenant(tenant_id)
        |> Ash.update(actor: actor(tenant_id), domain: Mezzanine.Work)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp find_work_object_by_external_ref(tenant_id, program_id, external_ref) do
    WorkObject
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(program_id == ^program_id and external_ref == ^external_ref)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [work_object | _]} -> {:ok, work_object}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp refresh_plan(work_object, tenant_id, attrs) do
    with {:ok, _prior_plan} <- maybe_supersede_plan(tenant_id, work_object.current_plan_id) do
      compile_attrs =
        case map_value(attrs, :policy_bundle_id) do
          nil -> %{}
          policy_bundle_id -> %{policy_bundle_id: policy_bundle_id}
        end

      WorkObject.compile_plan(work_object, compile_attrs,
        actor: actor(tenant_id),
        tenant: tenant_id
      )
    end
  end

  defp intake_attrs(attrs, program_id, work_class_id, external_ref) do
    %{
      program_id: program_id,
      work_class_id: work_class_id,
      external_ref: external_ref,
      title: map_value(attrs, :title) || external_ref,
      description: map_value(attrs, :description),
      priority: map_value(attrs, :priority) || 50,
      source_kind: map_value(attrs, :source_kind) || "external",
      payload:
        map_value(attrs, :payload) || Map.drop(attrs, [:tenant_id, :program_id, :work_class_id]),
      normalized_payload:
        map_value(attrs, :normalized_payload) ||
          map_value(attrs, :payload) ||
          Map.drop(attrs, [:tenant_id, :program_id, :work_class_id])
    }
  end

  defp refresh_intake_attrs(attrs, work_class_id, external_ref) do
    %{
      work_class_id: work_class_id,
      external_ref: external_ref,
      title: map_value(attrs, :title) || external_ref,
      description: map_value(attrs, :description),
      priority: map_value(attrs, :priority) || 50,
      source_kind: map_value(attrs, :source_kind) || "external",
      payload:
        map_value(attrs, :payload) || Map.drop(attrs, [:tenant_id, :program_id, :work_class_id]),
      normalized_payload:
        map_value(attrs, :normalized_payload) ||
          map_value(attrs, :payload) ||
          Map.drop(attrs, [:tenant_id, :program_id, :work_class_id])
    }
  end

  defp active_work_object?(work_object, filters) do
    work_object.status in @active_statuses and
      match_filter(work_object.status, Map.get(filters, :statuses)) and
      match_filter(work_object.source_kind, map_value(filters, :source_kind)) and
      match_filter(work_object.work_class_id, map_value(filters, :work_class_id))
  end

  defp match_filter(_value, nil), do: true
  defp match_filter(value, values) when is_list(values), do: value in values
  defp match_filter(value, expected), do: value == expected

  defp fetch_work_object(tenant_id, work_object_id) do
    WorkObject
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [work_object]} -> {:ok, work_object}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp runtime_execution(%ExecutionRecord{} = execution, _latest_execution), do: {:ok, execution}
  defp runtime_execution(nil, %ExecutionRecord{} = execution), do: {:ok, execution}
  defp runtime_execution(nil, nil), do: {:error, :runtime_projection_not_found}

  defp runtime_projection(%{
         tenant_id: tenant_id,
         work_object: work_object,
         current_plan: current_plan,
         active_run: active_run,
         execution: execution,
         pending_reviews: pending_reviews,
         gate_status: gate_status,
         projection_facts: projection_facts,
         opts: opts
       }) do
    computed_at =
      execution.updated_at
      |> Kernel.||(execution.inserted_at)
      |> Kernel.||(work_object.updated_at)
      |> Kernel.||(DateTime.utc_now())
      |> runtime_timestamp()

    %{
      subject_id: work_object.id,
      subject_kind: work_object.source_kind || "work_object",
      lifecycle_state: lifecycle_state(work_object, active_run, execution),
      work_status: work_object.status,
      run_status: active_run_status(active_run),
      review_status: gate_status.status,
      projection_name: @runtime_projection_name,
      projection_version: 1,
      projection_kind: "mezzanine_current_execution",
      computed_at: computed_at,
      updated_at: computed_at,
      subject: runtime_subject(work_object),
      source_binding: source_binding_projection(tenant_id, work_object, execution),
      source_bindings: [source_binding_projection(tenant_id, work_object, execution)],
      execution: execution_projection(execution),
      lower_receipt: lower_receipt_projection(execution),
      runtime: runtime_projection_facts(work_object, current_plan, active_run, execution, opts),
      evidence: evidence_projection(execution),
      review: review_projection(pending_reviews, gate_status),
      run: run_projection(execution),
      lower_envelope: lower_envelope_projection(execution),
      governance: governance_projection(execution),
      prompt: prompt_projection(execution),
      memory_context: memory_context_projection(execution),
      authority: authority_projection(execution),
      source_publication: source_publication_projection(execution),
      github_pr: github_pr_projection(execution),
      acceptance: acceptance_projection(execution),
      available_actions: [],
      queue: %{
        "pending_obligations" => normalize_value(projection_facts.pending_obligations),
        "blocking_conditions" => normalize_value(projection_facts.blocking_conditions),
        "next_step_preview" => normalize_value(projection_facts.next_step_preview)
      }
    }
  end

  defp runtime_subject(work_object) do
    %{
      "subject_id" => work_object.id,
      "subject_kind" => work_object.source_kind || "work_object",
      "lifecycle_state" => normalize_state(work_object.status),
      "status" => normalize_state(work_object.status),
      "title" => work_object.title,
      "external_ref" => work_object.external_ref
    }
    |> compact_map()
  end

  defp source_binding_projection(tenant_id, work_object, execution) do
    source_payload = source_payload(work_object)

    binding_ref =
      map_value(source_payload, :source_binding_id) ||
        execution.binding_snapshot
        |> map_value(:source_binding_refs)
        |> List.wrap()
        |> List.first()

    source_kind =
      map_value(source_payload, :provider) ||
        map_value(source_payload, :source_kind) ||
        work_object.source_kind ||
        "source"

    %{
      "binding_ref" => binding_ref || "#{source_kind}_primary",
      "source_ref" => source_ref(tenant_id, work_object, source_payload),
      "source_kind" => source_kind,
      "external_system" => map_value(source_payload, :provider) || source_kind,
      "source_state" =>
        map_value(source_payload, :source_state) || normalize_state(work_object.status),
      "source_url" => map_value(source_payload, :source_url),
      "workpad_refs" => source_workpad_refs(source_payload, execution),
      "metadata" =>
        %{
          "external_ref" => work_object.external_ref,
          "work_class_id" => work_object.work_class_id,
          "program_id" => work_object.program_id,
          "provider_external_ref" => map_value(source_payload, :provider_external_ref),
          "provider_revision" => map_value(source_payload, :provider_revision),
          "branch_ref" => map_value(source_payload, :branch_ref),
          "labels" => map_value(source_payload, :labels),
          "blocker_refs" => map_value(source_payload, :blocker_refs),
          "state_mapping" => map_value(source_payload, :state_mapping)
        }
        |> Map.merge(source_publication_projection(execution))
        |> compact_map()
    }
    |> compact_map()
  end

  defp source_ref(tenant_id, work_object, source_payload) do
    case map_value(source_payload, :source_ref) || map_value(source_payload, :external_ref) ||
           work_object.external_ref do
      value when is_binary(value) and value != "" ->
        if String.contains?(value, "://") do
          value
        else
          "source://#{work_object.source_kind || "source"}/#{tenant_id}/#{value}"
        end

      _other ->
        "source://#{work_object.source_kind || "source"}/#{tenant_id}/#{work_object.id}"
    end
  end

  defp source_workpad_refs(source_payload, execution) do
    source_payload
    |> map_value(:workpad_refs)
    |> List.wrap()
    |> Kernel.++(List.wrap(map_value(source_payload, :workpad_ref)))
    |> Kernel.++(workpad_refs(execution))
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.uniq()
  end

  defp execution_projection(execution) do
    %{
      "execution_id" => execution.id,
      "dispatch_state" => normalize_state(execution.dispatch_state),
      "trace_id" => execution.trace_id,
      "causation_id" => execution.causation_id,
      "recipe_ref" => execution.recipe_ref,
      "submission_dedupe_key" => execution.submission_dedupe_key,
      "updated_at" => runtime_timestamp(execution.updated_at || execution.inserted_at),
      "metadata" =>
        %{
          "installation_id" => execution.installation_id,
          "compiled_pack_revision" => execution.compiled_pack_revision,
          "dispatch_attempt_count" => execution.dispatch_attempt_count,
          "scheduler_state" => scheduler_state(execution),
          "claim_state" => claim_state(execution),
          "running_state" => running_state(execution),
          "retry_state" => retry_state(execution),
          "completion_state" => completion_state(execution),
          "next_dispatch_at" => runtime_timestamp(execution.next_dispatch_at),
          "last_dispatch_error_kind" => execution.last_dispatch_error_kind,
          "workflow_start_ref" => map_value(execution.dispatch_envelope, :workflow_start_ref),
          "workflow_ref" => map_value(execution.dispatch_envelope, :workflow_id)
        }
        |> compact_map()
    }
    |> compact_map()
  end

  defp lower_receipt_projection(%ExecutionRecord{lower_receipt: receipt} = execution)
       when is_map(receipt) and map_size(receipt) > 0 do
    refs = lower_receipt_refs(receipt, execution)

    %{
      "receipt_ref" => refs.receipt_ref,
      "receipt_id" => refs.receipt_id,
      "receipt_state" => refs.receipt_state,
      "lower_receipt_ref" => refs.lower_receipt_ref,
      "run_id" => map_value(receipt, :run_id),
      "attempt_id" => map_value(receipt, :attempt_id),
      "metadata" => Map.drop(receipt, ["provider_response", :provider_response])
    }
    |> compact_map()
  end

  defp lower_receipt_projection(%ExecutionRecord{} = execution) do
    %{
      "receipt_ref" => "lower-receipt://pending/#{execution.id}",
      "receipt_id" => "lower-receipt://pending/#{execution.id}",
      "receipt_state" => normalize_state(execution.dispatch_state),
      "lower_receipt_ref" => "lower-receipt://pending/#{execution.id}",
      "metadata" => %{
        "pending_reason" => "workflow_start_queued",
        "workflow_start_ref" => map_value(execution.dispatch_envelope, :workflow_start_ref)
      }
    }
  end

  defp lower_receipt_refs(receipt, execution) do
    fallback_ref = "lower-receipt://#{execution.id}"
    receipt_ref = receipt_value(receipt, [:receipt_ref, :receipt_id], fallback_ref)

    %{
      receipt_ref: receipt_ref,
      receipt_id: receipt_value(receipt, [:receipt_id, :receipt_ref], receipt_ref),
      receipt_state:
        receipt_value(
          receipt,
          [:receipt_state, :state],
          normalize_state(execution.dispatch_state)
        ),
      lower_receipt_ref: receipt_value(receipt, [:lower_receipt_ref, :receipt_ref], receipt_ref)
    }
  end

  defp receipt_value(receipt, keys, default) do
    Enum.find_value(keys, &map_value(receipt, &1)) || default
  end

  defp runtime_projection_facts(work_object, current_plan, active_run, execution, opts) do
    receipt = lower_receipt_map(execution)

    %{
      "token_totals" => map_value(receipt, :token_totals) || %{},
      "token_dedupe" => map_value(receipt, :token_dedupe) || %{},
      "rate_limit" => map_value(receipt, :rate_limit) || %{},
      "retry_queue" => map_value(receipt, :retry) || retry_queue_projection(execution),
      "event_counts" => lower_event_counts(receipt) || runtime_event_counts(execution),
      "aitrace" => map_value(receipt, :aitrace) || %{},
      "metadata" =>
        %{
          "work_object_id" => work_object.id,
          "plan_id" => current_plan_id(current_plan),
          "run_id" => active_run_id(active_run),
          "scheduler_state" => scheduler_state(execution),
          "claim_state" => claim_state(execution),
          "running_state" => running_state(execution),
          "retry_state" => retry_state(execution),
          "completion_state" => completion_state(execution),
          "projection_source" => "mezzanine_work_queries",
          "projection_mode" => Keyword.get(opts, :projection_mode, "same_run_readback"),
          "lower_envelope" => lower_envelope_projection(execution),
          "governance" => governance_projection(execution),
          "memory_context" => memory_context_projection(execution),
          "acceptance" => acceptance_projection(execution),
          "github_pr" => github_pr_projection(execution),
          "source_publication" => source_publication_projection(execution)
        }
        |> compact_map()
    }
  end

  defp run_projection(execution) do
    receipt = lower_receipt_map(execution)
    runtime_profile = map_value(receipt, :runtime_profile) || %{}

    %{
      "run_ref" => map_value(receipt, :run_id) || map_value(receipt, :run_ref),
      "attempt_ref" => map_value(receipt, :attempt_id) || map_value(receipt, :attempt_ref),
      "runtime_profile_ref" => map_value(runtime_profile, :runtime_profile_ref),
      "runtime_profile_kind" => map_value(runtime_profile, :runtime_profile_kind)
    }
    |> compact_map()
  end

  defp lower_envelope_projection(execution) do
    receipt = lower_receipt_map(execution)

    case map_value(receipt, :governed_lower_envelope) do
      %{} = envelope -> envelope
      _other -> map_value(receipt, :lower_envelope) || %{}
    end
  end

  defp governance_projection(execution) do
    receipt = lower_receipt_map(execution)
    runtime_profile = map_value(receipt, :runtime_profile) || %{}
    authority = map_value(receipt, :authority_decision) || %{}

    %{
      "runtime_profile_ref" => map_value(runtime_profile, :runtime_profile_ref),
      "runtime_profile_kind" => map_value(runtime_profile, :runtime_profile_kind),
      "authority_ref" => map_value(authority, :authority_ref),
      "authority_decision_hash" => map_value(authority, :authority_decision_hash),
      "connector_manifest_refs" =>
        receipt
        |> map_value(:connector_manifests)
        |> List.wrap()
        |> Enum.flat_map(&projection_ref(&1, :connector_manifest_ref)),
      "capability_negotiation_refs" =>
        receipt
        |> map_value(:capability_negotiations)
        |> List.wrap()
        |> Enum.flat_map(&projection_ref(&1, :capability_negotiation_ref))
    }
    |> compact_map()
  end

  defp prompt_projection(execution) do
    execution |> lower_receipt_map() |> map_value(:prompt_provenance) || %{}
  end

  defp memory_context_projection(execution) do
    execution |> lower_receipt_map() |> map_value(:memory_context) || %{}
  end

  defp authority_projection(execution) do
    receipt = lower_receipt_map(execution)

    %{
      "authority_decision" => map_value(receipt, :authority_decision),
      "provider_account" => map_value(receipt, :provider_account),
      "credential" => map_value(receipt, :credential)
    }
    |> compact_map()
  end

  defp source_publication_projection(execution) do
    execution |> lower_receipt_map() |> map_value(:source_publication) || %{}
  end

  defp github_pr_projection(execution) do
    execution |> lower_receipt_map() |> map_value(:github_pr_evidence) || %{}
  end

  defp acceptance_projection(execution) do
    execution |> lower_receipt_map() |> map_value(:acceptance) || %{}
  end

  defp workpad_refs(execution) do
    execution
    |> lower_receipt_map()
    |> map_value(:workpad_refs)
    |> List.wrap()
  end

  defp lower_event_counts(receipt) do
    events = receipt |> map_value(:runtime_events) |> List.wrap()

    if events == [] do
      nil
    else
      Enum.frequencies_by(events, &(map_value(&1, :event_kind) || "unknown"))
    end
  end

  defp projection_ref(%{} = row, key), do: row |> map_value(key) |> List.wrap()
  defp projection_ref(value, _key) when is_binary(value), do: [value]
  defp projection_ref(_value, _key), do: []

  defp runtime_event_counts(execution) do
    %{
      "workflow_start_queued" => 1,
      "execution_#{normalize_state(execution.dispatch_state)}" => 1
    }
    |> Map.put("scheduler_#{scheduler_state(execution)}", 1)
  end

  defp retry_queue_projection(execution) do
    if retry_scheduled?(execution) do
      attempt_number = retry_attempt_number(execution)
      metadata = normalize_value(execution.last_dispatch_error_payload || %{})
      scheduled_at = runtime_timestamp(execution.next_dispatch_at)

      [
        %{
          "retry_ref" => "retry://#{execution.id}/#{attempt_number}",
          "attempt_ref" => "attempt://#{execution.id}/#{attempt_number}",
          "status" => "scheduled",
          "reason" => execution.last_dispatch_error_kind,
          "scheduled_at" => scheduled_at,
          "due_at" => map_value(metadata, :due_at) || scheduled_at,
          "delay_ms" => map_value(metadata, :delay_ms),
          "delay_type" => map_value(metadata, :delay_type),
          "continuation?" => map_value(metadata, :continuation?),
          "worker_ref" => map_value(metadata, :worker_ref),
          "workspace_ref" => map_value(metadata, :workspace_ref),
          "last_error_ref" =>
            "execution-error://#{execution.id}/#{execution.last_dispatch_error_kind}",
          "metadata" => metadata
        }
        |> compact_map()
      ]
    else
      []
    end
  end

  defp scheduler_state(execution) do
    cond do
      retry_scheduled?(execution) ->
        "retry_scheduled"

      execution.dispatch_state == :queued ->
        "claim_queued"

      execution.dispatch_state in [:in_flight, :accepted_active] ->
        "running"

      execution.dispatch_state == :completed ->
        "completed"

      execution.dispatch_state == :failed ->
        "failed"

      execution.dispatch_state == :cancelled ->
        "cancelled"

      execution.dispatch_state == :rejected ->
        "rejected"

      true ->
        normalize_state(execution.dispatch_state)
    end
  end

  defp claim_state(execution) do
    cond do
      retry_scheduled?(execution) ->
        "released"

      execution.dispatch_state in [:queued, :in_flight, :accepted_active] ->
        "claimed"

      execution.dispatch_state == :completed ->
        "completed"

      execution.dispatch_state in [:cancelled, :failed, :rejected] ->
        "released"

      true ->
        normalize_state(execution.dispatch_state)
    end
  end

  defp running_state(execution) do
    cond do
      retry_scheduled?(execution) -> "not_running"
      execution.dispatch_state in [:in_flight, :accepted_active] -> @running_projection_state
      true -> "not_running"
    end
  end

  defp retry_state(execution), do: if(retry_scheduled?(execution), do: "scheduled", else: "none")

  defp completion_state(%ExecutionRecord{dispatch_state: :completed}), do: "completed"
  defp completion_state(_execution), do: nil

  defp retry_scheduled?(execution) do
    not is_nil(execution.next_dispatch_at) and
      is_binary(execution.last_dispatch_error_kind) and
      execution.last_dispatch_error_kind != ""
  end

  defp retry_attempt_number(execution), do: (execution.dispatch_attempt_count || 0) + 1

  defp evidence_projection(execution) do
    %{
      "evidence_refs" =>
        [
          %{
            "evidence_ref" =>
              map_value(execution.dispatch_envelope, :workflow_start_evidence_ref) ||
                "execution-evidence://#{execution.id}/workflow-start",
            "evidence_kind" => "workflow_start",
            "status" => "present",
            "metadata" => %{
              "workflow_start_ref" => map_value(execution.dispatch_envelope, :workflow_start_ref)
            }
          }
        ] ++ lower_evidence_refs(execution)
    }
  end

  defp lower_evidence_refs(execution) do
    execution
    |> lower_receipt_map()
    |> map_value(:artifact_refs)
    |> List.wrap()
    |> Enum.flat_map(fn
      %{} = ref ->
        [
          %{
            "evidence_ref" => map_value(ref, :content_ref),
            "evidence_kind" => map_value(ref, :kind),
            "content_ref" => map_value(ref, :content_ref),
            "status" => "verified",
            "metadata" => %{"collector_ref" => map_value(ref, :collector_ref)}
          }
          |> compact_map()
        ]

      _other ->
        []
    end)
  end

  defp review_projection(pending_reviews, gate_status) do
    %{
      "status" => normalize_state(gate_status.status),
      "pending_decision_ids" => Enum.map(pending_reviews, & &1.id),
      "metadata" => %{
        "release_ready" => gate_status.release_ready?
      }
    }
  end

  defp lifecycle_state(_work_object, %Run{status: :running}, _execution), do: "running"

  defp lifecycle_state(work_object, _active_run, execution) do
    case execution.dispatch_state do
      :queued -> "queued"
      :in_flight -> "running"
      :accepted_active -> "running"
      :completed -> "completed"
      :failed -> "failed"
      :cancelled -> "cancelled"
      _other -> normalize_state(work_object.status)
    end
  end

  defp lower_receipt_map(%ExecutionRecord{lower_receipt: receipt})
       when is_map(receipt) and map_size(receipt) > 0,
       do: receipt

  defp lower_receipt_map(_execution), do: %{}

  defp fetch_current_plan(_tenant_id, nil), do: {:ok, nil}

  defp fetch_current_plan(tenant_id, plan_id) do
    WorkPlan
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^plan_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [plan]} -> {:ok, plan}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_supersede_plan(_tenant_id, nil), do: {:ok, nil}

  defp maybe_supersede_plan(tenant_id, plan_id) do
    with {:ok, %WorkPlan{} = plan} <- fetch_current_plan(tenant_id, plan_id) do
      WorkPlan.supersede(plan, actor: actor(tenant_id), tenant: tenant_id)
    end
  end

  defp list_run_series(tenant_id, work_object_id) do
    RunSeries.list_for_work_object(work_object_id, actor: actor(tenant_id), tenant: tenant_id)
  end

  defp fetch_active_run(_tenant_id, []), do: {:ok, nil}

  defp fetch_active_run(tenant_id, [series | _]) do
    case series.current_run_id do
      nil -> {:ok, nil}
      run_id -> fetch_run(tenant_id, run_id)
    end
  end

  defp fetch_active_execution(subject_id) do
    case ExecutionRecord.active_for_subject(subject_id) do
      {:ok, [execution | _]} -> {:ok, execution}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_latest_execution(subject_id) do
    ExecutionRecord
    |> Ash.Query.filter(subject_id == ^subject_id)
    |> Ash.read(authorize?: false, domain: Mezzanine.Execution)
    |> case do
      {:ok, executions} ->
        {:ok, Enum.max_by(executions, &execution_sort_key/1, fn -> nil end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_run(tenant_id, run_id) do
    Run
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^run_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
    |> case do
      {:ok, [run]} -> {:ok, run}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_pending_reviews_for_work(tenant_id, work_object_id) do
    ReviewUnit.list_for_work_object(work_object_id, actor: actor(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, review_units} ->
        {:ok, Enum.filter(review_units, &(&1.status in [:pending, :in_review, :escalated]))}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_control_session(tenant_id, work_object_id) do
    WorkControl.control_session_for_work(tenant_id, work_object_id)
  end

  defp open_escalation_count(tenant_id, work_objects) do
    work_ids = Enum.map(work_objects, & &1.subject_id)

    Escalation
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id in ^work_ids and status == :open)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
    |> case do
      {:ok, escalations} -> {:ok, length(escalations)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stalled_count(tenant_id, work_objects) do
    run_ids =
      work_objects
      |> Enum.map(&fetch_current_run_id(tenant_id, &1.subject_id))
      |> Enum.filter(&is_binary/1)

    if run_ids == [] do
      {:ok, 0}
    else
      Run
      |> Ash.Query.set_tenant(tenant_id)
      |> Ash.Query.filter(id in ^run_ids and status == :stalled)
      |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Runs)
      |> case do
        {:ok, runs} -> {:ok, length(runs)}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp fetch_current_run_id(tenant_id, work_object_id) do
    case list_run_series(tenant_id, work_object_id) do
      {:ok, [series | _]} -> series.current_run_id
      _ -> nil
    end
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

  defp obligation_ids(nil), do: []
  defp obligation_ids(plan), do: plan.obligation_ids || []

  defp latest_evidence_bundle_id(audit_report) do
    audit_report.evidence_bundles
    |> Enum.reverse()
    |> List.first()
    |> case do
      nil -> nil
      evidence_bundle -> evidence_bundle.id
    end
  end

  defp last_event_at([]), do: nil

  defp last_event_at(timeline) do
    timeline
    |> Enum.reverse()
    |> List.first()
    |> case do
      nil -> nil
      event -> Map.get(event, :occurred_at)
    end
  end

  defp current_plan_id(nil), do: nil
  defp current_plan_id(plan), do: plan.id

  defp current_plan_status(nil), do: nil
  defp current_plan_status(plan), do: plan.status

  defp active_run_id(nil), do: nil
  defp active_run_id(run), do: run.id

  defp active_run_status(nil), do: nil
  defp active_run_status(run), do: run.status

  defp active_execution_id(nil), do: nil
  defp active_execution_id(execution), do: execution.id

  defp active_execution_dispatch_state(nil), do: nil
  defp active_execution_dispatch_state(execution), do: execution.dispatch_state

  defp active_execution_trace_id(nil), do: nil
  defp active_execution_trace_id(execution), do: execution.trace_id

  defp latest_execution_id(nil), do: nil
  defp latest_execution_id(execution), do: execution.id

  defp latest_execution_dispatch_state(nil), do: nil
  defp latest_execution_dispatch_state(execution), do: execution.dispatch_state

  defp latest_execution_trace_id(nil), do: nil
  defp latest_execution_trace_id(execution), do: execution.trace_id

  defp control_session_id(nil), do: nil
  defp control_session_id(control_session), do: control_session.id

  defp control_mode(nil), do: nil
  defp control_mode(control_session), do: control_session.current_mode

  defp execution_sort_key(execution) do
    {execution.updated_at || execution.inserted_at, execution.inserted_at, execution.id}
  end

  defp fetch_string(attrs, opts, key) do
    ServiceSupport.fetch_string(attrs, opts, key, {:missing_required, key})
  end

  defp normalize_state(nil), do: nil
  defp normalize_state(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_state(value), do: value

  defp compact_map(map), do: Map.reject(map, fn {_key, value} -> value in [nil, [], %{}] end)

  defp runtime_timestamp(nil), do: nil
  defp runtime_timestamp(%DateTime{} = value), do: value

  defp runtime_timestamp(%NaiveDateTime{} = value),
    do: DateTime.from_naive!(value, "Etc/UTC")

  defp runtime_timestamp(value), do: value

  defp normalize_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp normalize_value(value) when is_boolean(value), do: value
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)

  defp normalize_value(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} -> {key, normalize_value(nested_value)} end)
  end

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value), do: value

  defp source_payload(work_object) do
    normalized_payload = map_value(work_object, :normalized_payload) || %{}
    payload = map_value(normalized_payload, :payload) || map_value(work_object, :payload) || %{}

    Map.merge(normalized_payload, payload)
  end

  defp map_value(map, key), do: ServiceSupport.map_value(map, key)
  defp actor(tenant_id), do: ServiceSupport.actor(tenant_id)
end
