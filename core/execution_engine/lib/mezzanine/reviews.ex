defmodule Mezzanine.Reviews do
  @moduledoc """
  Neutral review, waiver, escalation, and release-readiness services.
  """

  require Ash.Query

  alias Mezzanine.Audit.WorkAudit
  alias Mezzanine.Evidence.EvidenceItem
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Review.{Escalation, ReviewDecision, ReviewUnit, Waiver}
  alias Mezzanine.Runs.{Run, RunArtifact, RunSeries}
  alias Mezzanine.ServiceSupport
  alias Mezzanine.Work.WorkObject

  @open_review_statuses [:pending, :in_review, :escalated]
  @terminal_run_statuses [:completed, :failed, :cancelled]
  @terminal_work_statuses [:completed, :cancelled]
  @review_due_in_seconds 86_400

  @spec list_pending_reviews(String.t()) :: {:ok, [struct()]} | {:error, term()}
  def list_pending_reviews(tenant_id) when is_binary(tenant_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(status in [:pending, :in_review, :escalated])
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  @spec review_detail(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def review_detail(tenant_id, review_unit_id)
      when is_binary(tenant_id) and is_binary(review_unit_id) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, decisions} <- list_decisions(tenant_id, review_unit.id),
         {:ok, waivers} <- list_waivers(tenant_id, review_unit.id),
         {:ok, escalations} <- list_escalations(tenant_id, review_unit.id) do
      {:ok,
       %{
         review_unit: review_unit,
         decisions: decisions,
         waivers: waivers,
         escalations: escalations,
         gate_status: evaluate_gate_status([review_unit], escalations)
       }}
    end
  end

  @spec gate_status(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def gate_status(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, review_units} <- review_units_for_work(tenant_id, work_object_id),
         {:ok, escalations} <- escalations_for_work(tenant_id, work_object_id) do
      {:ok, evaluate_gate_status(review_units, escalations)}
    end
  end

  @spec release_ready?(String.t(), Ecto.UUID.t()) :: {:ok, boolean()} | {:error, term()}
  def release_ready?(tenant_id, work_object_id)
      when is_binary(tenant_id) and is_binary(work_object_id) do
    with {:ok, gate_status} <- gate_status(tenant_id, work_object_id) do
      {:ok, gate_status.release_ready?}
    end
  end

  @spec pending_review_summaries(String.t(), Ecto.UUID.t()) ::
          {:ok, [map()]} | {:error, term()}
  def pending_review_summaries(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, work_index} <- program_work_index(tenant_id, program_id),
         {:ok, review_units} <- list_pending_reviews(tenant_id) do
      summaries =
        review_units
        |> Enum.filter(&Map.has_key?(work_index, &1.work_object_id))
        |> Enum.map(fn review_unit ->
          work_object = Map.fetch!(work_index, review_unit.work_object_id)
          ref = subject_ref(work_object.id)

          %{
            decision_ref: decision_ref(review_unit.id, ref, review_unit.review_kind),
            subject_ref: ref,
            status: ServiceSupport.normalize_state(review_unit.status),
            required_by: review_unit.required_by,
            summary: work_object.title,
            payload: %{
              reviewer_actor: ServiceSupport.normalize_value(review_unit.reviewer_actor),
              review_kind: ServiceSupport.normalize_state(review_unit.review_kind)
            }
          }
        end)
        |> Enum.sort_by(&{&1.required_by || DateTime.utc_now(), &1.decision_ref.id})

      {:ok, summaries}
    end
  end

  @spec review_detail_projection(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  def review_detail_projection(tenant_id, review_unit_id)
      when is_binary(tenant_id) and is_binary(review_unit_id) do
    with {:ok, review_detail} <- review_detail(tenant_id, review_unit_id),
         %ReviewUnit{} = review_unit <- review_detail.review_unit,
         subject_ref = subject_ref(review_unit.work_object_id),
         {:ok, work_object} <- fetch_work_object(tenant_id, review_unit.work_object_id),
         {:ok, run} <- fetch_review_run(tenant_id, review_unit),
         {:ok, audit_report} <- WorkAudit.work_report(tenant_id, review_unit.work_object_id),
         {:ok, evidence_items} <- list_evidence_items(tenant_id, review_unit.evidence_bundle_id),
         {:ok, run_artifacts} <- list_run_artifacts(tenant_id, review_unit.run_id) do
      {:ok,
       %{
         decision_ref: decision_ref(review_unit.id, subject_ref, review_unit.review_kind),
         subject_ref: subject_ref,
         status: ServiceSupport.normalize_state(review_unit.status),
         required_by: review_unit.required_by,
         summary: work_object.title,
         payload: %{
           review_kind: ServiceSupport.normalize_state(review_unit.review_kind),
           reviewer_actor: ServiceSupport.normalize_value(review_unit.reviewer_actor),
           review_unit: ServiceSupport.normalize_value(review_unit),
           work_object: ServiceSupport.normalize_value(work_object),
           run: ServiceSupport.normalize_value(run),
           evidence_bundle:
             ServiceSupport.normalize_value(select_evidence_bundle(audit_report, review_unit)),
           evidence_items: ServiceSupport.normalize_value(evidence_items),
           run_artifacts: ServiceSupport.normalize_value(run_artifacts),
           audit_timeline: %{
             work_object_id: work_object.id,
             timeline: ServiceSupport.normalize_value(audit_report.timeline),
             audit_events: ServiceSupport.normalize_value(audit_report.audit_events)
           },
           gate_status: ServiceSupport.normalize_value(review_detail.gate_status),
           decisions: ServiceSupport.normalize_value(review_detail.decisions),
           waivers: ServiceSupport.normalize_value(review_detail.waivers),
           escalations: ServiceSupport.normalize_value(review_detail.escalations)
         }
       }}
    end
  end

  @spec recover_execution(String.t(), ExecutionRecord.t() | Ecto.UUID.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def recover_execution(tenant_id, execution_or_id, opts \\ [])
      when is_binary(tenant_id) and is_list(opts) do
    with {:ok, execution} <- load_execution(execution_or_id),
         :ok <- ensure_semantic_failure(execution),
         {:ok, work_object} <- fetch_work_object(tenant_id, execution.subject_id),
         {:ok, run} <- fetch_active_run_for_work(tenant_id, work_object.id),
         {:ok, run, run_failed?} <- maybe_mark_run_failed(tenant_id, run, opts),
         {:ok, work_object} <- maybe_mark_awaiting_review(tenant_id, work_object),
         {:ok, review_unit, review_created?} <-
           ensure_recovery_review(tenant_id, work_object, run, execution, opts),
         {:ok, _run_audit} <-
           maybe_record_run_failed_audit(
             tenant_id,
             work_object,
             run,
             execution,
             run_failed?,
             opts
           ),
         {:ok, _review_audit} <-
           maybe_record_review_created_audit(
             tenant_id,
             work_object,
             run,
             review_unit,
             execution,
             review_created?,
             opts
           ),
         {:ok, _timeline} <- WorkAudit.refresh_timeline(tenant_id, work_object.id, now(opts)) do
      {:ok,
       %{
         execution: execution,
         work_object: work_object,
         run: run,
         review_unit: review_unit,
         review_created?: review_created?
       }}
    end
  end

  @spec record_decision(String.t(), Ecto.UUID.t(), map()) :: {:ok, map()} | {:error, term()}
  def record_decision(tenant_id, review_unit_id, attrs)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(attrs) do
    decision = Map.get(attrs, :decision)

    if decision in [:accept, :reject] do
      with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
           {:ok, decision_record} <- create_decision(tenant_id, review_unit_id, attrs),
           {:ok, updated_review_unit} <- transition_review_unit(tenant_id, review_unit, decision),
           {:ok, _audit} <- record_review_audit(tenant_id, review_unit, decision, attrs) do
        {:ok, %{review_unit: updated_review_unit, decision: decision_record}}
      end
    else
      {:error, :unsupported_decision}
    end
  end

  @spec waive_review(String.t(), Ecto.UUID.t(), map()) :: {:ok, map()} | {:error, term()}
  def waive_review(tenant_id, review_unit_id, attrs)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(attrs) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, waiver} <- create_waiver(tenant_id, review_unit, attrs),
         {:ok, updated_review_unit} <- transition_review_unit(tenant_id, review_unit, :waive),
         {:ok, _audit} <- record_review_audit(tenant_id, review_unit, :waive, attrs) do
      {:ok, %{review_unit: updated_review_unit, waiver: waiver}}
    end
  end

  @spec escalate_review(String.t(), Ecto.UUID.t(), map()) :: {:ok, map()} | {:error, term()}
  def escalate_review(tenant_id, review_unit_id, attrs)
      when is_binary(tenant_id) and is_binary(review_unit_id) and is_map(attrs) do
    with {:ok, review_unit} <- fetch_review_unit(tenant_id, review_unit_id),
         {:ok, escalation} <- create_escalation(tenant_id, review_unit, attrs),
         {:ok, updated_review_unit} <- transition_review_unit(tenant_id, review_unit, :escalate),
         {:ok, _audit} <- record_review_audit(tenant_id, review_unit, :escalate, attrs) do
      {:ok, %{review_unit: updated_review_unit, escalation: escalation}}
    end
  end

  defp record_review_audit(tenant_id, review_unit, decision, attrs) do
    WorkAudit.record_event(tenant_id, %{
      program_id: Map.fetch!(attrs, :program_id),
      work_object_id: review_unit.work_object_id,
      review_unit_id: review_unit.id,
      event_kind: audit_event_for(decision),
      actor_kind: Map.get(attrs, :actor_kind, :human),
      actor_ref: Map.get(attrs, :actor_ref, "reviewer"),
      payload: Map.get(attrs, :payload, %{})
    })
  end

  defp create_decision(tenant_id, review_unit_id, attrs) do
    ReviewDecision
    |> Ash.Changeset.for_create(:record_decision, %{
      review_unit_id: review_unit_id,
      decision: Map.fetch!(attrs, :decision),
      actor_kind: Map.get(attrs, :actor_kind, :human),
      actor_ref: Map.get(attrs, :actor_ref, "reviewer"),
      reason: Map.get(attrs, :reason),
      payload: Map.get(attrs, :payload, %{}),
      decided_at: Map.get(attrs, :decided_at, DateTime.utc_now())
    })
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
  end

  defp create_waiver(tenant_id, review_unit, attrs) do
    if waiver_active?(Map.get(attrs, :expires_at, DateTime.utc_now())) do
      Waiver
      |> Ash.Changeset.for_create(:grant_waiver, %{
        review_unit_id: review_unit.id,
        work_object_id: review_unit.work_object_id,
        reason: Map.fetch!(attrs, :reason),
        granted_by: Map.fetch!(attrs, :actor_ref),
        expires_at: Map.get(attrs, :expires_at),
        conditions: Map.get(attrs, :conditions, [])
      })
      |> Ash.Changeset.set_tenant(tenant_id)
      |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
    else
      {:error, :expired_waiver}
    end
  end

  defp create_escalation(tenant_id, review_unit, attrs) do
    Escalation
    |> Ash.Changeset.for_create(:raise_escalation, %{
      review_unit_id: review_unit.id,
      work_object_id: review_unit.work_object_id,
      reason: Map.get(attrs, :reason),
      escalated_by: Map.get(attrs, :actor_ref),
      assigned_to: Map.get(attrs, :assigned_to),
      priority: Map.get(attrs, :priority, :normal)
    })
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.create(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
  end

  defp transition_review_unit(tenant_id, review_unit, :accept) do
    transition_review_unit(tenant_id, review_unit, :accept, %{})
  end

  defp transition_review_unit(tenant_id, review_unit, :reject) do
    transition_review_unit(tenant_id, review_unit, :reject, %{})
  end

  defp transition_review_unit(tenant_id, review_unit, :waive) do
    transition_review_unit(tenant_id, review_unit, :waive, %{})
  end

  defp transition_review_unit(tenant_id, review_unit, :escalate) do
    transition_review_unit(tenant_id, review_unit, :escalate, %{})
  end

  defp transition_review_unit(tenant_id, review_unit, action, attrs) do
    review_unit
    |> Ash.Changeset.for_update(action, attrs)
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Review)
  end

  defp review_units_for_work(tenant_id, work_object_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp escalations_for_work(tenant_id, work_object_id) do
    Escalation
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(work_object_id == ^work_object_id and status == :open)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp list_decisions(tenant_id, review_unit_id) do
    ReviewDecision
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(review_unit_id == ^review_unit_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp list_waivers(tenant_id, review_unit_id) do
    Waiver
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(review_unit_id == ^review_unit_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp list_escalations(tenant_id, review_unit_id) do
    Escalation
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(review_unit_id == ^review_unit_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
  end

  defp fetch_review_unit(tenant_id, review_unit_id) do
    ReviewUnit
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^review_unit_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Review)
    |> case do
      {:ok, [review_unit]} -> {:ok, review_unit}
      {:ok, []} -> {:error, :not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp evaluate_gate_status(review_units, escalations)
       when is_list(review_units) and is_list(escalations) do
    counts = Enum.frequencies_by(review_units, & &1.status)
    open_escalation_count = Enum.count(escalations, &(&1.status == :open))

    status =
      cond do
        open_escalation_count > 0 -> :escalated
        Map.get(counts, :rejected, 0) > 0 -> :rejected
        Map.get(counts, :pending, 0) > 0 or Map.get(counts, :in_review, 0) > 0 -> :pending
        review_units == [] -> :clear
        true -> :approved
      end

    %{
      status: status,
      pending_count: Map.get(counts, :pending, 0) + Map.get(counts, :in_review, 0),
      accepted_count: Map.get(counts, :accepted, 0),
      waived_count: Map.get(counts, :waived, 0),
      rejected_count: Map.get(counts, :rejected, 0),
      escalated_count: open_escalation_count,
      release_ready?: status in [:approved, :clear]
    }
  end

  defp waiver_active?(expires_at, now \\ DateTime.utc_now())
  defp waiver_active?(nil, _now), do: true

  defp waiver_active?(%DateTime{} = expires_at, now) do
    DateTime.compare(expires_at, now) in [:gt, :eq]
  end

  defp audit_event_for(:accept), do: :review_accepted
  defp audit_event_for(:reject), do: :review_rejected
  defp audit_event_for(:waive), do: :review_waived
  defp audit_event_for(:escalate), do: :escalation_raised

  defp program_work_index(tenant_id, program_id) do
    WorkObject.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, work_objects} -> {:ok, Map.new(work_objects, &{&1.id, &1})}
      {:error, reason} -> {:error, reason}
    end
  end

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

  defp fetch_review_run(tenant_id, %ReviewUnit{run_id: run_id}) when is_binary(run_id),
    do: fetch_run(tenant_id, run_id)

  defp fetch_review_run(tenant_id, %ReviewUnit{work_object_id: work_object_id}),
    do: fetch_active_run_for_work(tenant_id, work_object_id)

  defp fetch_active_run_for_work(tenant_id, work_object_id) do
    with {:ok, run_series} <-
           RunSeries.list_for_work_object(
             work_object_id,
             actor: actor(tenant_id),
             tenant: tenant_id
           ) do
      fetch_active_run(tenant_id, run_series)
    end
  end

  defp fetch_active_run(_tenant_id, []), do: {:ok, nil}

  defp fetch_active_run(tenant_id, [series | _]) do
    case series.current_run_id do
      nil -> {:ok, nil}
      run_id -> fetch_run(tenant_id, run_id)
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

  defp list_evidence_items(_tenant_id, nil), do: {:ok, []}

  defp list_evidence_items(tenant_id, evidence_bundle_id) do
    EvidenceItem
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(evidence_bundle_id == ^evidence_bundle_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Evidence)
  end

  defp list_run_artifacts(_tenant_id, nil), do: {:ok, []}

  defp list_run_artifacts(tenant_id, run_id) do
    RunArtifact.list_for_run(run_id, actor: actor(tenant_id), tenant: tenant_id)
  end

  defp select_evidence_bundle(audit_report, %ReviewUnit{evidence_bundle_id: evidence_bundle_id}) do
    audit_report.evidence_bundles
    |> Enum.find(&(&1.id == evidence_bundle_id))
    |> case do
      nil -> List.first(Enum.reverse(audit_report.evidence_bundles))
      evidence_bundle -> evidence_bundle
    end
  end

  defp load_execution(%ExecutionRecord{} = execution), do: {:ok, execution}

  defp load_execution(execution_id) when is_binary(execution_id) do
    ExecutionRecord
    |> Ash.Query.filter(id == ^execution_id)
    |> Ash.read_one(authorize?: false, domain: Mezzanine.Execution)
    |> case do
      {:ok, %ExecutionRecord{} = execution} -> {:ok, execution}
      {:ok, nil} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_semantic_failure(%ExecutionRecord{failure_kind: :semantic_failure}), do: :ok
  defp ensure_semantic_failure(_execution), do: {:error, :unsupported_recovery_target}

  defp maybe_mark_run_failed(_tenant_id, nil, _opts), do: {:ok, nil, false}

  defp maybe_mark_run_failed(_tenant_id, %Run{status: status} = run, _opts)
       when status in @terminal_run_statuses do
    {:ok, run, false}
  end

  defp maybe_mark_run_failed(tenant_id, %Run{} = run, opts) do
    run
    |> Ash.Changeset.for_update(:record_failed, %{
      completed_at: now(opts),
      result_summary: "Semantic failure requires operator recovery"
    })
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Runs)
    |> case do
      {:ok, %Run{} = failed_run} -> {:ok, failed_run, true}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_mark_awaiting_review(
         _tenant_id,
         %WorkObject{status: :awaiting_review} = work_object
       ),
       do: {:ok, work_object}

  defp maybe_mark_awaiting_review(_tenant_id, %WorkObject{status: status})
       when status in @terminal_work_statuses do
    {:error, :unsupported_recovery_target}
  end

  defp maybe_mark_awaiting_review(tenant_id, %WorkObject{} = work_object) do
    work_object
    |> Ash.Changeset.for_update(:mark_awaiting_review, %{})
    |> Ash.Changeset.set_tenant(tenant_id)
    |> Ash.update(actor: actor(tenant_id), authorize?: false, domain: Mezzanine.Work)
  end

  defp ensure_recovery_review(tenant_id, %WorkObject{} = work_object, run, execution, opts) do
    with {:ok, review_units} <-
           ReviewUnit.list_for_work_object(work_object.id,
             actor: actor(tenant_id),
             tenant: tenant_id
           ) do
      case Enum.find(review_units, &recovery_review?(&1, run_id(run), execution.id)) do
        %ReviewUnit{} = review_unit ->
          {:ok, review_unit, false}

        nil ->
          create_recovery_review(tenant_id, work_object, run, execution, opts)
      end
    end
  end

  defp recovery_review?(%ReviewUnit{} = review_unit, run_id, execution_id) do
    review_unit.status in @open_review_statuses and
      review_unit.review_kind == :operator_review and
      review_unit.run_id == run_id and
      ServiceSupport.map_value(review_unit.decision_profile, :recovery_kind) == "semantic_failure" and
      ServiceSupport.map_value(review_unit.decision_profile, :execution_id) == execution_id
  end

  defp create_recovery_review(tenant_id, %WorkObject{} = work_object, run, execution, opts) do
    required_by = DateTime.add(now(opts), @review_due_in_seconds, :second)

    ReviewUnit.create_review_unit(
      %{
        work_object_id: work_object.id,
        run_id: run_id(run),
        review_kind: :operator_review,
        required_by: required_by,
        decision_profile: %{
          "recovery_kind" => "semantic_failure",
          "execution_id" => execution.id,
          "failure_kind" => "semantic_failure",
          "recipe_ref" => execution.recipe_ref,
          "trace_id" => execution.trace_id,
          "options" => ["accept", "reject", "escalate"]
        },
        reviewer_actor: %{
          "kind" => "human",
          "ref" => Keyword.get(opts, :reviewer_ref, "operator")
        }
      },
      actor: actor(tenant_id),
      tenant: tenant_id
    )
    |> case do
      {:ok, %ReviewUnit{} = review_unit} -> {:ok, review_unit, true}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_record_run_failed_audit(
         _tenant_id,
         _work_object,
         nil,
         _execution,
         _run_failed?,
         _opts
       ),
       do: {:ok, nil}

  defp maybe_record_run_failed_audit(
         _tenant_id,
         _work_object,
         _run,
         _execution,
         false,
         _opts
       ),
       do: {:ok, nil}

  defp maybe_record_run_failed_audit(
         tenant_id,
         %WorkObject{} = work_object,
         %Run{} = run,
         execution,
         true,
         opts
       ) do
    WorkAudit.record_event(tenant_id, %{
      program_id: work_object.program_id,
      work_object_id: work_object.id,
      run_id: run.id,
      event_kind: :run_failed,
      actor_kind: :system,
      actor_ref: actor_ref(opts),
      payload: %{
        execution_id: execution.id,
        failure_kind: "semantic_failure",
        recipe_ref: execution.recipe_ref
      },
      occurred_at: now(opts)
    })
  end

  defp maybe_record_review_created_audit(
         _tenant_id,
         _work_object,
         _run,
         _review_unit,
         _execution,
         false,
         _opts
       ),
       do: {:ok, nil}

  defp maybe_record_review_created_audit(
         tenant_id,
         %WorkObject{} = work_object,
         run,
         %ReviewUnit{} = review_unit,
         execution,
         true,
         opts
       ) do
    WorkAudit.record_event(tenant_id, %{
      program_id: work_object.program_id,
      work_object_id: work_object.id,
      run_id: run_id(run),
      review_unit_id: review_unit.id,
      event_kind: :review_created,
      actor_kind: :system,
      actor_ref: actor_ref(opts),
      payload: %{
        execution_id: execution.id,
        failure_kind: "semantic_failure",
        recipe_ref: execution.recipe_ref
      },
      occurred_at: now(opts)
    })
  end

  defp subject_ref(subject_id), do: %{id: subject_id, subject_kind: "work_object"}

  defp decision_ref(review_unit_id, subject_ref, review_kind) do
    %{
      id: review_unit_id,
      decision_kind: ServiceSupport.normalize_state(review_kind),
      subject_ref: subject_ref
    }
  end

  defp run_id(nil), do: nil
  defp run_id(%Run{id: run_id}), do: run_id

  defp actor_ref(opts), do: Keyword.get(opts, :actor_ref, "semantic_failure_recovery")
  defp now(opts), do: Keyword.get(opts, :now, DateTime.utc_now())
  defp actor(tenant_id), do: %{tenant_id: tenant_id}
end
