defmodule Mezzanine.AppKitBridge.SemanticFailureRecoveryService do
  @moduledoc """
  Bridge-owned deterministic recovery for post-acceptance semantic failures.

  The neutral execution ledger already records `:semantic_failure`. This
  service turns that durable failure into an operator-facing recovery state for
  the AppKit operational surfaces without reopening the outbox.
  """

  require Ash.Query

  alias Mezzanine.AppKitBridge.AdapterSupport
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Work.WorkObject
  alias Mezzanine.WorkAudit

  @open_review_statuses [:pending, :in_review, :escalated]
  @terminal_run_statuses [:completed, :failed, :cancelled]
  @terminal_work_statuses [:completed, :cancelled]
  @review_due_in_seconds 86_400

  @spec recover_execution(String.t(), ExecutionRecord.t() | Ecto.UUID.t()) ::
          {:ok, map()} | {:error, term()}
  def recover_execution(tenant_id, execution_or_id)
      when is_binary(tenant_id) do
    recover_execution(tenant_id, execution_or_id, [])
  end

  @spec recover_execution(String.t(), ExecutionRecord.t() | Ecto.UUID.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def recover_execution(tenant_id, execution_or_id, opts \\ [])
      when is_binary(tenant_id) and is_list(opts) do
    with {:ok, execution} <- load_execution(execution_or_id),
         :ok <- ensure_semantic_failure(execution),
         {:ok, work_object} <- fetch_work_object(tenant_id, execution.subject_id),
         {:ok, run} <- fetch_active_run(tenant_id, work_object.id),
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

  defp fetch_work_object(tenant_id, work_object_id) do
    WorkObject
    |> Ash.get(
      work_object_id,
      actor: actor(tenant_id),
      authorize?: false,
      domain: Mezzanine.Work,
      tenant: tenant_id
    )
    |> case do
      {:ok, %WorkObject{} = work_object} -> {:ok, work_object}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_active_run(tenant_id, work_object_id) do
    case RunSeries.list_for_work_object(work_object_id,
           actor: actor(tenant_id),
           tenant: tenant_id
         ) do
      {:ok, run_series} ->
        run_series
        |> Enum.find(&is_binary(&1.current_run_id))
        |> case do
          nil -> {:ok, nil}
          series -> fetch_run(tenant_id, series.current_run_id)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_run(tenant_id, run_id) do
    Run
    |> Ash.get(
      run_id,
      actor: actor(tenant_id),
      authorize?: false,
      domain: Mezzanine.Runs,
      tenant: tenant_id
    )
    |> case do
      {:ok, %Run{} = run} -> {:ok, run}
      {:error, reason} -> {:error, reason}
    end
  end

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
      AdapterSupport.map_value(review_unit.decision_profile, :recovery_kind) == "semantic_failure" and
      AdapterSupport.map_value(review_unit.decision_profile, :execution_id) == execution_id
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

  defp run_id(nil), do: nil
  defp run_id(%Run{id: run_id}), do: run_id

  defp actor_ref(opts), do: Keyword.get(opts, :actor_ref, "semantic_failure_recovery")
  defp actor(tenant_id), do: AdapterSupport.actor(tenant_id)
  defp now(opts), do: Keyword.get(opts, :now, DateTime.utc_now())
end
