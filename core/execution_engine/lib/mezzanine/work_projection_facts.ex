defmodule Mezzanine.WorkProjectionFacts do
  @moduledoc """
  Derives first-class obligation, blocker, and next-step facts from substrate
  truth for operator-facing projections.
  """

  alias Mezzanine.ServiceSupport

  @terminal_work_statuses ["completed", "cancelled", "failed"]
  @queued_run_statuses ["pending", "scheduled"]
  @active_run_statuses ["running"]
  @blocked_review_statuses ["pending", "in_review", "escalated", "rejected"]

  @spec build(map(), map() | nil, map() | nil, [map()], map() | nil, map() | nil) :: map()
  def build(work_object, current_plan, active_run, pending_reviews, control_session, gate_status)
      when is_list(pending_reviews) do
    gate_status = normalize_gate_status(gate_status)

    pending_obligations =
      pending_obligations(current_plan, pending_reviews, gate_status)
      |> Enum.map(&normalize_value/1)

    blocking_conditions =
      blocking_conditions(
        work_object,
        pending_obligations,
        pending_reviews,
        control_session,
        gate_status
      )
      |> Enum.map(&normalize_value/1)

    next_step_preview =
      next_step_preview(
        work_object,
        current_plan,
        active_run,
        pending_obligations,
        blocking_conditions,
        control_session,
        gate_status
      )
      |> normalize_value()

    %{
      pending_obligations: pending_obligations,
      blocking_conditions: blocking_conditions,
      next_step_preview: next_step_preview
    }
  end

  defp pending_obligations(nil, pending_reviews, gate_status) do
    review_unit_obligations(pending_reviews, gate_status)
  end

  defp pending_obligations(current_plan, pending_reviews, gate_status) do
    case review_intents(current_plan) do
      [] ->
        review_unit_obligations(pending_reviews, gate_status)

      intents ->
        Enum.map(intents, fn intent ->
          review_kind = review_kind_for_gate(map_value(intent, :gate))

          matched_review =
            Enum.find(pending_reviews, &(normalize_state(&1.review_kind) == review_kind))

          required_decisions = required_decisions(intent, matched_review)
          obligation_status = obligation_status(matched_review, gate_status)

          %{
            obligation_id: obligation_id(intent),
            obligation_kind: "review",
            status: obligation_status,
            summary: obligation_summary(map_value(intent, :gate), obligation_status),
            decision_ref_id: matched_review && matched_review.id,
            required_by: matched_review && matched_review.required_by,
            blocking?: obligation_blocking?(matched_review, gate_status),
            metadata:
              compact_map(%{
                source: "work_plan",
                intent_id: map_value(intent, :intent_id),
                gate: normalize_state(map_value(intent, :gate)),
                review_kind: review_kind,
                required_decisions: required_decisions,
                review_unit_id: matched_review && matched_review.id,
                review_status: matched_review && normalize_state(matched_review.status)
              })
          }
        end)
    end
  end

  defp review_unit_obligations(pending_reviews, gate_status) do
    Enum.map(pending_reviews, fn review ->
      review_status = normalize_state(review.status)

      %{
        obligation_id: "review-unit:#{review.id}",
        obligation_kind: "review",
        status: review_status,
        summary: obligation_summary(review.review_kind, review_status),
        decision_ref_id: review.id,
        required_by: review.required_by,
        blocking?: gate_blocking?(gate_status),
        metadata:
          compact_map(%{
            source: "review_unit",
            gate: normalize_state(review.review_kind),
            review_kind: normalize_state(review.review_kind),
            required_decisions: required_decisions(%{}, review),
            review_unit_id: review.id
          })
      }
    end)
  end

  defp blocking_conditions(
         work_object,
         pending_obligations,
         pending_reviews,
         control_session,
         gate_status
       ) do
    []
    |> maybe_add_dependency_blocker(work_object)
    |> maybe_add_pause_blocker(control_session)
    |> Kernel.++(review_blockers(pending_obligations, pending_reviews, gate_status))
  end

  defp maybe_add_dependency_blocker(blockers, work_object) do
    blocked_by_work_id = map_value(work_object, :blocked_by_work_id)

    if normalize_state(map_value(work_object, :status)) == "blocked" or
         is_binary(blocked_by_work_id) do
      blockers ++
        [
          %{
            blocker_kind: "dependency_blocked",
            status: "blocked",
            summary: "A dependency must be resolved before this subject may proceed.",
            reason: "dependent_work_object_not_terminal",
            obligation_id: nil,
            decision_ref_id: nil,
            metadata: compact_map(%{blocked_by_work_id: blocked_by_work_id})
          }
        ]
    else
      blockers
    end
  end

  defp maybe_add_pause_blocker(blockers, nil), do: blockers

  defp maybe_add_pause_blocker(blockers, control_session) do
    if normalize_state(map_value(control_session, :current_mode)) == "paused" do
      blockers ++
        [
          %{
            blocker_kind: "operator_paused",
            status: "blocked",
            summary: "The subject is paused and must be resumed before dispatch continues.",
            reason: "subject_paused",
            obligation_id: nil,
            decision_ref_id: nil,
            metadata:
              compact_map(%{
                control_session_id: map_value(control_session, :id),
                control_mode: normalize_state(map_value(control_session, :current_mode))
              })
          }
        ]
    else
      blockers
    end
  end

  defp review_blockers(pending_obligations, pending_reviews, gate_status) do
    case normalize_state(map_value(gate_status, :status)) do
      "pending" ->
        pending_obligations
        |> Enum.filter(&(&1.status in ["pending", "in_review"]))
        |> Enum.map(&review_blocker("review_pending", &1, "review_pending"))
        |> fallback_gate_blocker("review_pending", pending_reviews, gate_status)

      "escalated" ->
        pending_obligations
        |> Enum.filter(&(&1.status == "escalated"))
        |> Enum.map(&review_blocker("review_escalated", &1, "review_escalated"))
        |> fallback_gate_blocker("review_escalated", pending_reviews, gate_status)

      "rejected" ->
        pending_obligations
        |> Enum.filter(&(&1.status == "rejected"))
        |> Enum.map(&review_blocker("review_rejected", &1, "review_rejected"))
        |> fallback_gate_blocker("review_rejected", pending_reviews, gate_status)

      _other ->
        []
    end
  end

  defp fallback_gate_blocker([], blocker_kind, pending_reviews, gate_status) do
    review_unit = List.first(pending_reviews)

    [
      %{
        blocker_kind: blocker_kind,
        status: "blocked",
        summary: fallback_review_blocker_summary(blocker_kind),
        reason: normalize_state(map_value(gate_status, :status)),
        obligation_id: nil,
        decision_ref_id: review_unit && review_unit.id,
        metadata: %{}
      }
    ]
  end

  defp fallback_gate_blocker(blockers, _blocker_kind, _pending_reviews, _gate_status),
    do: blockers

  defp review_blocker(blocker_kind, obligation, reason) do
    %{
      blocker_kind: blocker_kind,
      status: "blocked",
      summary: obligation.summary,
      reason: reason,
      obligation_id: obligation.obligation_id,
      decision_ref_id: obligation.decision_ref_id,
      metadata:
        compact_map(%{
          obligation_status: obligation.status,
          review_kind: map_value(obligation.metadata, :review_kind)
        })
    }
  end

  defp next_step_preview(
         work_object,
         current_plan,
         active_run,
         pending_obligations,
         blocking_conditions,
         control_session,
         gate_status
       ) do
    context = %{
      work_object: work_object,
      current_plan: current_plan,
      pending_obligations: pending_obligations,
      control_session: control_session,
      gate_status: gate_status,
      blocker_kinds: Enum.map(blocking_conditions, & &1.blocker_kind),
      obligation_ids: Enum.map(pending_obligations, & &1.obligation_id),
      run_status: normalize_state(active_run && map_value(active_run, :status))
    }

    blocking_step_preview(context) ||
      terminal_step_preview(context) ||
      run_step_preview(context) ||
      plan_step_preview(context)
  end

  defp blocking_step_preview(%{blocker_kinds: blocker_kinds} = context) do
    cond do
      "operator_paused" in blocker_kinds ->
        step_preview(
          "resume_subject",
          "blocked",
          "Resume the paused subject before dispatch may continue.",
          context.blocker_kinds,
          context.obligation_ids,
          %{control_mode: normalize_state(map_value(context.control_session, :current_mode))}
        )

      "dependency_blocked" in blocker_kinds ->
        step_preview(
          "resolve_dependency",
          "blocked",
          "Resolve the blocking dependency before this subject may proceed.",
          context.blocker_kinds,
          context.obligation_ids,
          %{blocked_by_work_id: map_value(context.work_object, :blocked_by_work_id)}
        )

      "review_rejected" in blocker_kinds ->
        step_preview(
          "resolve_rejected_review",
          "blocked",
          "Resolve the rejected review outcome before execution may proceed.",
          context.blocker_kinds,
          context.obligation_ids,
          %{gate_status: normalize_state(map_value(context.gate_status, :status))}
        )

      "review_escalated" in blocker_kinds ->
        step_preview(
          "resolve_escalation",
          "blocked",
          "Resolve the open escalation before execution may proceed.",
          context.blocker_kinds,
          context.obligation_ids,
          %{gate_status: normalize_state(map_value(context.gate_status, :status))}
        )

      "review_pending" in blocker_kinds ->
        step_preview(
          "record_review_decision",
          "blocked",
          "Record the pending review decision before execution may proceed.",
          context.blocker_kinds,
          context.obligation_ids,
          %{gate_status: normalize_state(map_value(context.gate_status, :status))}
        )

      true ->
        nil
    end
  end

  defp terminal_step_preview(context) do
    work_status = normalize_state(map_value(context.work_object, :status))

    if work_status in @terminal_work_statuses do
      step_preview(
        "none",
        "complete",
        "The subject is terminal and has no further planned work.",
        context.blocker_kinds,
        context.obligation_ids,
        %{work_status: work_status}
      )
    end
  end

  defp run_step_preview(%{run_status: run_status} = context) do
    cond do
      run_status in @queued_run_statuses ->
        step_preview(
          "dispatch_execution",
          "in_progress",
          "The current execution is queued for dispatch.",
          context.blocker_kinds,
          context.obligation_ids,
          %{run_status: run_status}
        )

      run_status in (@active_run_statuses ++ ["stalled"]) ->
        step_preview(
          "await_execution_outcome",
          if(run_status == "stalled", do: "blocked", else: "in_progress"),
          if(run_status == "stalled",
            do: "Investigate the stalled execution before the subject may proceed.",
            else: "Await lower execution outcome and receipt reconciliation."
          ),
          context.blocker_kinds,
          context.obligation_ids,
          %{run_status: run_status}
        )

      run_status == "failed" ->
        step_preview(
          "investigate_failed_run",
          "blocked",
          "Investigate the failed execution and resolve any recovery review.",
          context.blocker_kinds,
          context.obligation_ids,
          %{run_status: run_status}
        )

      true ->
        nil
    end
  end

  defp plan_step_preview(%{current_plan: nil} = context) do
    step_preview(
      "compile_plan",
      "ready",
      "Compile a work plan before execution may proceed.",
      context.blocker_kinds,
      context.obligation_ids,
      %{}
    )
  end

  defp plan_step_preview(context) do
    run_intent = first_run_intent(context.current_plan)

    step_preview(
      "start_run",
      "ready",
      "Start the next governed execution for this subject.",
      context.blocker_kinds,
      context.obligation_ids,
      %{
        intent_id: map_value(run_intent, :intent_id),
        capability: map_value(run_intent, :capability),
        review_required: context.pending_obligations != []
      }
    )
  end

  defp step_preview(
         step_kind,
         status,
         summary,
         blocking_condition_kinds,
         obligation_ids,
         metadata
       ) do
    %{
      step_kind: step_kind,
      status: status,
      summary: summary,
      blocking_condition_kinds: blocking_condition_kinds,
      obligation_ids: obligation_ids,
      metadata: compact_map(metadata)
    }
  end

  defp obligation_status(nil, gate_status) do
    case normalize_state(map_value(gate_status, :status)) do
      status when status in @blocked_review_statuses -> status
      _other -> "pending"
    end
  end

  defp obligation_status(review, _gate_status), do: normalize_state(review.status)

  defp obligation_blocking?(nil, gate_status), do: gate_blocking?(gate_status)

  defp obligation_blocking?(review, _gate_status),
    do: normalize_state(review.status) in @blocked_review_statuses

  defp required_decisions(intent, nil) do
    map_value(intent, :required_decisions) || 1
  end

  defp required_decisions(_intent, review) do
    map_value(review.decision_profile, :required_decisions) || 1
  end

  defp obligation_id(intent) do
    case map_value(intent, :intent_id) do
      intent_id when is_binary(intent_id) and intent_id != "" -> "obligation:#{intent_id}"
      _other -> "obligation:unknown"
    end
  end

  defp obligation_summary(gate, "rejected") do
    "#{humanize_gate(gate)} review rejected and must be resolved before execution may proceed."
  end

  defp obligation_summary(gate, "escalated") do
    "#{humanize_gate(gate)} review escalated and requires operator follow-up."
  end

  defp obligation_summary(gate, _status) do
    "#{humanize_gate(gate)} review is required before execution may proceed."
  end

  defp fallback_review_blocker_summary("review_pending"),
    do: "A pending review decision is blocking further execution."

  defp fallback_review_blocker_summary("review_escalated"),
    do: "An escalated review is blocking further execution."

  defp fallback_review_blocker_summary("review_rejected"),
    do: "A rejected review is blocking further execution."

  defp fallback_review_blocker_summary(_other),
    do: "A review condition is blocking further execution."

  defp review_intents(current_plan), do: map_value(current_plan, :derived_review_intents) || []

  defp first_run_intent(current_plan) do
    case map_value(current_plan, :derived_run_intents) || [] do
      [intent | _rest] -> intent
      _other -> %{}
    end
  end

  defp review_kind_for_gate(nil), do: "operator_review"
  defp review_kind_for_gate("operator"), do: "operator_review"
  defp review_kind_for_gate(:operator), do: "operator_review"

  defp review_kind_for_gate(gate) do
    gate
    |> normalize_state()
    |> Kernel.<>("_review")
  end

  defp humanize_gate(nil), do: "Operator"

  defp humanize_gate(gate) do
    gate
    |> normalize_state()
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp gate_blocking?(gate_status) do
    normalize_state(map_value(gate_status, :status)) in ["pending", "escalated", "rejected"]
  end

  defp normalize_gate_status(nil), do: %{status: "clear", release_ready?: true}
  defp normalize_gate_status(gate_status), do: gate_status

  defp compact_map(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp normalize_state(nil), do: nil
  defp normalize_state(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_state(value), do: value
  defp normalize_value(value), do: ServiceSupport.normalize_value(value)
  defp map_value(map, key), do: ServiceSupport.map_value(map, key)
end
