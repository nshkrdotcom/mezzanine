defmodule Mezzanine.WorkflowRuntime.AgentLoop do
  @moduledoc """
  Provider-neutral M2 agent-turn runtime.

  Phase 5 keeps semantic compute and lower execution deterministic. The loop
  owns turn sequencing and emits S0/M1-readable rows; products still enter
  through AppKit and lower work still runs behind the Mezzanine runtime
  boundary.
  """

  alias Mezzanine.AgentRuntime.{
    AgentLoopProjection,
    AgentRunSpec,
    AgentTurnState,
    RuntimeCommandResult,
    RuntimeEventRow,
    Support,
    ToolActionReceipt,
    ToolActionRequest
  }

  @timestamp ~U[2026-04-27 00:00:00Z]
  @activity_sequence [
    :wake_and_pin,
    :recall,
    :assemble_context,
    :reflect,
    :govern,
    :submit_lower_run,
    :await_execution_outcome,
    :semanticize_outcome,
    :commit_private_memory,
    :advance_turn
  ]

  @doc "Static M2 contract used by registry, review, and harness tests."
  @spec contract() :: map()
  def contract do
    %{
      workflow_module: Mezzanine.Workflows.AgentLoop,
      mechanism: "M2",
      activity_sequence: @activity_sequence,
      lower_attempt_linkage: %{
        strategy: :outbox_activity_handoff,
        lower_workflow: Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow,
        outcome_signal_buffering?: true
      },
      activity_owners: %{
        wake_and_pin: :mezzanine,
        recall: :outer_brain,
        assemble_context: :outer_brain,
        reflect: :outer_brain,
        govern: :citadel,
        submit_lower_run: :jido_integration,
        await_execution_outcome: :execution_plane,
        semanticize_outcome: :outer_brain,
        commit_private_memory: :mezzanine,
        advance_turn: :mezzanine
      },
      signal_names: ["approve", "deny", "input", "pause", "resume", "cancel", "replan", "rework"],
      continue_as_new: %{
        threshold_key: :continue_as_new_turn_threshold,
        default_turn_threshold: 50,
        identity_ref: :run_ref
      }
    }
  end

  @doc "Runs one deterministic provider-free M2 turn."
  @spec run(term()) :: {:ok, AgentLoopProjection.t()} | {:error, term()}
  def run(input) do
    with {:ok, pinned} <- wake_and_pin_activity(input),
         {:ok, recalled} <- recall_activity(pinned),
         {:ok, assembled} <- assemble_context_activity(recalled),
         {:ok, reflected} <- reflect_activity(assembled),
         {:ok, governed} <- govern_activity(reflected),
         {:ok, submitted} <- submit_lower_run_activity(governed),
         {:ok, observed} <- await_execution_outcome_activity(submitted),
         {:ok, semanticized} <- semanticize_outcome_activity(observed),
         {:ok, memory_checked} <- commit_private_memory_activity(semanticized),
         do: advance_turn_activity(memory_checked)
  end

  @doc "Initial replay-safe signal state for an AgentLoop run."
  @spec initial_state(term()) :: map()
  def initial_state(input) do
    attrs = normalize(input)

    %{
      run_ref: map_value(attrs, :run_ref, "run://unknown"),
      signal_state: "initialized",
      seen_signal_keys: MapSet.new(),
      buffered_outcomes: %{}
    }
  end

  @doc "Applies an ordered operator signal with idempotency-key suppression."
  @spec apply_signal(map(), term()) :: {:ok, map()} | {:error, term()}
  def apply_signal(state, signal) when is_map(state) do
    signal = normalize(signal)
    idempotency_key = map_value(signal, :idempotency_key, map_value(signal, :signal_id))
    seen = Map.get(state, :seen_signal_keys, MapSet.new())

    cond do
      not present?(idempotency_key) ->
        {:error, :invalid_agent_loop_signal}

      MapSet.member?(seen, idempotency_key) ->
        {:ok, %{state | signal_state: "duplicate_suppressed"}}

      true ->
        {:ok,
         state
         |> Map.put(:seen_signal_keys, MapSet.put(seen, idempotency_key))
         |> Map.put(:signal_state, "accepted")
         |> Map.put(:last_signal_ref, map_value(signal, :signal_id))}
    end
  end

  def apply_signal(_state, _signal), do: {:error, :invalid_agent_loop_signal}

  @doc "Buffers lower outcomes that arrive before the matching submission wait is active."
  @spec buffer_outcome_signal(map(), term()) :: {:ok, map()} | {:error, term()}
  def buffer_outcome_signal(state, outcome) when is_map(state) do
    outcome = normalize(outcome)
    submission_ref = map_value(outcome, :lower_submission_ref)

    if present?(submission_ref) do
      buffered =
        state
        |> Map.get(:buffered_outcomes, %{})
        |> Map.put_new(submission_ref, outcome)

      {:ok, %{state | buffered_outcomes: buffered}}
    else
      {:error, :invalid_lower_outcome_signal}
    end
  end

  def buffer_outcome_signal(_state, _outcome), do: {:error, :invalid_lower_outcome_signal}

  @doc "Consumes a buffered lower outcome exactly once."
  @spec consume_buffered_outcome(map(), String.t()) :: {:ok, map(), map()} | {:error, term()}
  def consume_buffered_outcome(state, submission_ref) when is_map(state) do
    case Map.pop(Map.get(state, :buffered_outcomes, %{}), submission_ref) do
      {nil, _buffered} ->
        {:error, :missing_lower_outcome}

      {outcome, buffered} ->
        {:ok, outcome, %{state | buffered_outcomes: buffered}}
    end
  end

  def consume_buffered_outcome(_state, _submission_ref), do: {:error, :missing_lower_outcome}

  @spec wake_and_pin_activity(term()) :: {:ok, map()} | {:error, term()}
  def wake_and_pin_activity(input) do
    attrs = normalize(input)
    spec_attrs = Map.drop(attrs, [:fixture_script, :continue_as_new_turn_threshold])

    with {:ok, spec} <- AgentRunSpec.new(spec_attrs) do
      workflow_ref = "workflow://agent-loop/#{ref_suffix(spec.run_ref)}"
      turn_ref = "turn://agent-loop/#{ref_suffix(spec.run_ref)}/1"
      script = map_value(attrs, :fixture_script, "success_first_try")

      state =
        base_state(spec, workflow_ref, turn_ref)
        |> Map.put(:fixture_script, script)
        |> Map.put(
          :continue_as_new_turn_threshold,
          map_value(attrs, :continue_as_new_turn_threshold, 50)
        )
        |> put_event("agent_run.accepted", "agent run accepted")
        |> put_event("turn.started", "turn started")

      {:ok, state}
    end
  end

  @spec recall_activity(map()) :: {:ok, map()}
  def recall_activity(state) do
    {:ok,
     state
     |> Map.put(:context_refs, [])
     |> put_event("recall.completed", "recall completed")}
  end

  @spec assemble_context_activity(map()) :: {:ok, map()}
  def assemble_context_activity(state) do
    context_pack_ref = "context-pack://agent-loop/#{ref_suffix(state.turn_ref)}"

    {:ok,
     state
     |> Map.put(:context_pack_ref, context_pack_ref)
     |> put_event("context.assembled", "context assembled")}
  end

  @spec reflect_activity(map()) :: {:ok, map()} | {:error, term()}
  def reflect_activity(state) do
    request_attrs = %{
      action_ref: "action://agent-loop/#{ref_suffix(state.turn_ref)}",
      turn_ref: state.turn_ref,
      run_ref: state.run_ref,
      profile_ref: state.profile_ref,
      tool_ref: tool_ref_for(state.fixture_script),
      capability_ref: capability_ref_for(state.fixture_script),
      input_artifact_ref: "artifact://agent-loop/#{ref_suffix(state.turn_ref)}/input",
      authority_context_ref: "authority-context://agent-loop/#{ref_suffix(state.turn_ref)}",
      idempotency_key: "agent-run:authority:#{ref_suffix(state.turn_ref)}:action",
      trace_id: state.trace_id,
      workspace_ref: state.workspace_ref
    }

    with {:ok, request} <- ToolActionRequest.new(request_attrs) do
      {:ok,
       state
       |> Map.put(:action_request, request)
       |> Map.put(:reflection_ref, "reflection://agent-loop/#{ref_suffix(state.turn_ref)}")
       |> put_event("action.requested", "action requested")}
    end
  end

  @spec govern_activity(map()) :: {:ok, map()}
  def govern_activity(state) do
    decision =
      case state.fixture_script do
        "denied_write_then_allowed_read" -> :denied
        "approval_wait_then_submit" -> :approval_required
        _script -> :approved
      end

    event_kind =
      case decision do
        :denied -> "authority.denied"
        :approval_required -> "review.pending"
        :approved -> "authority.approved"
      end

    {:ok,
     state
     |> Map.put(:authority_decision, %{
       decision_ref: "authority-decision://agent-loop/#{ref_suffix(state.turn_ref)}",
       decision: decision,
       for_action_ref: state.action_request.action_ref
     })
     |> put_event(event_kind, String.replace(event_kind, ".", " "))}
  end

  @spec submit_lower_run_activity(map()) :: {:ok, map()}
  def submit_lower_run_activity(%{authority_decision: %{decision: :approved}} = state) do
    submission_ref = "lower-submission://agent-loop/#{ref_suffix(state.turn_ref)}"

    {:ok,
     state
     |> Map.put(:lower_submission_ref, submission_ref)
     |> Map.put(:submission_dedupe_key, "agent-run:lower:#{ref_suffix(state.turn_ref)}:decision")
     |> put_event("action.submitted", "action submitted")}
  end

  def submit_lower_run_activity(state), do: {:ok, state}

  @spec await_execution_outcome_activity(map()) :: {:ok, map()} | {:error, term()}
  def await_execution_outcome_activity(%{authority_decision: %{decision: :approved}} = state) do
    receipt_attrs = %{
      receipt_ref: "action-receipt://agent-loop/#{ref_suffix(state.turn_ref)}",
      action_ref: state.action_request.action_ref,
      turn_ref: state.turn_ref,
      status: :succeeded,
      lower_receipt_ref: "lower-receipt://agent-loop/#{ref_suffix(state.turn_ref)}",
      output_artifact_refs: ["artifact://agent-loop/#{ref_suffix(state.turn_ref)}/output"],
      evidence_refs: ["evidence://agent-loop/#{ref_suffix(state.turn_ref)}"],
      retry_posture: :none,
      trace_id: state.trace_id
    }

    with {:ok, receipt} <- ToolActionReceipt.new(receipt_attrs) do
      {:ok,
       state
       |> Map.put(:action_receipt, receipt)
       |> Map.put(:execution_outcome_ref, receipt.lower_receipt_ref)
       |> put_event("receipt.observed", "receipt observed")}
    end
  end

  def await_execution_outcome_activity(%{authority_decision: %{decision: :denied}} = state) do
    denied_receipt(state, :denied, "authority denied")
  end

  def await_execution_outcome_activity(
        %{authority_decision: %{decision: :approval_required}} = state
      ) do
    denied_receipt(state, :approval_required, "approval required")
  end

  @spec semanticize_outcome_activity(map()) :: {:ok, map()}
  def semanticize_outcome_activity(%{action_receipt: %{status: :succeeded}} = state) do
    {:ok,
     state
     |> Map.put(:candidate_fact_refs, [
       "candidate-fact://agent-loop/#{ref_suffix(state.turn_ref)}/1"
     ])
     |> put_event("outcome.semanticized", "outcome semanticized")}
  end

  def semanticize_outcome_activity(state) do
    {:ok, Map.put_new(state, :candidate_fact_refs, [])}
  end

  @spec commit_private_memory_activity(map()) :: {:ok, map()}
  def commit_private_memory_activity(state) do
    {:ok,
     state
     |> Map.put(:memory_proof_refs, [])
     |> Map.put(:memory_commit_state, "skipped_before_phase_7")
     |> put_event("memory.skipped", "private memory skipped before phase 7")}
  end

  @spec advance_turn_activity(map()) :: {:ok, AgentLoopProjection.t()} | {:error, term()}
  def advance_turn_activity(state) do
    terminal_state = terminal_state(state)
    final_turn_state = final_turn_state(state, terminal_state)
    runtime_events = put_event(state, "run.terminal", "run terminal").runtime_events
    receipt_ref_set = receipt_ref_set(state, runtime_events)

    AgentLoopProjection.new(%{
      run_ref: state.run_ref,
      subject_ref: state.subject_ref,
      workflow_ref: state.workflow_ref,
      session_ref: state.session_ref,
      workspace_ref: state.workspace_ref,
      worker_ref: state.worker_ref,
      terminal_state: terminal_state,
      current_turn_ref: state.turn_ref,
      status: terminal_state,
      turn_states: [final_turn_state],
      action_requests: [state.action_request],
      action_receipts: [state.action_receipt],
      runtime_events: runtime_events,
      command_results: [command_result(state, terminal_state)],
      budget_state: final_budget_state(state),
      candidate_fact_refs: Map.get(state, :candidate_fact_refs, []),
      memory_proof_refs: Map.get(state, :memory_proof_refs, []),
      receipt_ref_set: receipt_ref_set,
      diagnostics: diagnostics(state, receipt_ref_set)
    })
  end

  @doc "Turns an M2 projection into an M1-shaped readback payload."
  @spec to_runtime_run_detail(AgentLoopProjection.t()) :: {:ok, map()} | {:error, term()}
  def to_runtime_run_detail(%AgentLoopProjection{} = projection) do
    {:ok,
     %{
       "run_ref" => projection.run_ref,
       "subject_ref" => projection.subject_ref,
       "workflow_ref" => projection.workflow_ref,
       "status" => projection.status,
       "terminal_state" => projection.terminal_state,
       "current_turn_ref" => projection.current_turn_ref,
       "turns" => Enum.map(projection.turn_states, &AgentTurnState.dump/1),
       "action_requests" => Enum.map(projection.action_requests, &ToolActionRequest.dump/1),
       "action_receipts" => Enum.map(projection.action_receipts, &ToolActionReceipt.dump/1),
       "events" => Enum.map(projection.runtime_events, &RuntimeEventRow.dump/1),
       "command_results" => Enum.map(projection.command_results, &RuntimeCommandResult.dump/1),
       "budget_state" => Support.dump_value(projection.budget_state || %{}),
       "candidate_fact_refs" => projection.candidate_fact_refs || [],
       "memory_proof_refs" => projection.memory_proof_refs || [],
       "session_ref" => projection.session_ref,
       "workspace_ref" => projection.workspace_ref,
       "worker_ref" => projection.worker_ref,
       "receipt_ref_set" => Support.dump_value(projection.receipt_ref_set || %{})
     }}
  end

  def to_runtime_run_detail(_projection), do: {:error, :invalid_agent_loop_projection}

  defp base_state(spec, workflow_ref, turn_ref) do
    %{
      tenant_ref: spec.tenant_ref,
      installation_ref: spec.installation_ref,
      profile_ref: spec.profile_ref,
      subject_ref: spec.subject_ref,
      run_ref: spec.run_ref,
      trace_id: spec.trace_id,
      workflow_ref: workflow_ref,
      session_ref: spec.session_ref || "session://agent-loop/#{ref_suffix(spec.run_ref)}",
      workspace_ref: spec.workspace_ref || "workspace://agent-loop/#{ref_suffix(spec.run_ref)}",
      worker_ref: spec.worker_ref || "worker://agent-loop/#{ref_suffix(spec.run_ref)}/fixture",
      turn_ref: turn_ref,
      turn_index: 1,
      max_turns: spec.max_turns,
      budget_state: %{"turns_remaining" => spec.max_turns},
      runtime_events: []
    }
  end

  defp denied_receipt(state, status, summary) do
    with {:ok, receipt} <-
           ToolActionReceipt.new(%{
             receipt_ref: "action-receipt://agent-loop/#{ref_suffix(state.turn_ref)}",
             action_ref: state.action_request.action_ref,
             turn_ref: state.turn_ref,
             status: status,
             output_artifact_refs: [],
             evidence_refs: ["evidence://agent-loop/#{ref_suffix(state.turn_ref)}/authority"],
             retry_posture: :none,
             trace_id: state.trace_id
           }) do
      {:ok,
       state
       |> Map.put(:action_receipt, receipt)
       |> put_event("receipt.observed", summary)}
    end
  end

  defp final_turn_state(state, terminal_state) do
    AgentTurnState.new!(%{
      turn_ref: state.turn_ref,
      run_ref: state.run_ref,
      subject_ref: state.subject_ref,
      turn_index: 1,
      state: turn_state(terminal_state),
      started_at: @timestamp,
      ended_at: @timestamp,
      context_refs: Map.get(state, :context_refs, []),
      reflection_ref: Map.get(state, :reflection_ref),
      planned_action_ref: state.action_request.action_ref,
      tool_action_request_ref: state.action_request.action_ref,
      tool_action_receipt_ref: state.action_receipt.receipt_ref,
      authority_decision_ref: state.authority_decision.decision_ref,
      observation_ref: Map.get(state, :execution_outcome_ref),
      semantic_fact_refs: Map.get(state, :candidate_fact_refs, []),
      memory_commit_ref: nil,
      terminal_reason: terminal_state,
      causation_id: "causation://agent-loop/#{ref_suffix(state.turn_ref)}",
      snapshot_epoch: 1,
      budget_before: %{"turns_remaining" => state.max_turns},
      budget_after: final_budget_state(state),
      trace_id: state.trace_id
    })
  end

  defp terminal_state(%{authority_decision: %{decision: :denied}}), do: "blocked"

  defp terminal_state(%{authority_decision: %{decision: :approval_required}}),
    do: "review_pending"

  defp terminal_state(_state), do: "completed"

  defp turn_state("completed"), do: :completed
  defp turn_state("blocked"), do: :blocked
  defp turn_state("review_pending"), do: :awaiting_authority

  defp final_budget_state(%{authority_decision: %{decision: :approved}, max_turns: max_turns}),
    do: %{"turns_remaining" => max(max_turns - 1, 0)}

  defp final_budget_state(%{max_turns: max_turns}), do: %{"turns_remaining" => max_turns}

  defp command_result(state, terminal_state) do
    RuntimeCommandResult.new!(%{
      command_ref: "command-result://agent-loop/#{ref_suffix(state.turn_ref)}/start",
      command_kind: :submit_turn,
      status: terminal_state,
      accepted?: terminal_state in ["completed", "review_pending"],
      coalesced?: false,
      authority_state: state.authority_decision.decision,
      authority_refs: [state.authority_decision.decision_ref],
      workflow_effect_state: terminal_state,
      projection_state: "projected",
      trace_id: state.trace_id,
      correlation_id: state.turn_ref,
      idempotency_key: "agent-run:command:#{ref_suffix(state.turn_ref)}:start",
      message: terminal_state,
      diagnostics: []
    })
  end

  defp diagnostics(%{continue_as_new_turn_threshold: threshold} = state, receipt_ref_set) do
    [
      %{"continue_as_new_evaluated?" => true, "threshold" => threshold},
      %{
        "context_pack_ref" => Map.get(state, :context_pack_ref),
        "retention_policy_ref" => "retention-policy://agent-loop/intermediate-context/v1",
        "sweep_rule_ref" => "sweep-rule://agent-loop/context-pack/transient-v1",
        "pinned_ref_classes_retained" => ["evidence", "review", "trace", "memory", "incident"]
      },
      %{"fixture_ref_set_complete?" => fixture_ref_set_complete?(receipt_ref_set)}
    ]
  end

  defp receipt_ref_set(state, runtime_events) do
    %{
      "session_refs" => [state.session_ref],
      "turn_refs" => [state.turn_ref],
      "event_refs" => Enum.map(runtime_events, & &1.event_ref),
      "workspace_refs" => [state.workspace_ref],
      "worker_refs" => [state.worker_ref],
      "lower_refs" =>
        compact_refs([
          Map.get(state, :lower_submission_ref),
          state.action_receipt.lower_receipt_ref
        ]),
      "authority_refs" => compact_refs([state.authority_decision.decision_ref]),
      "outcome_refs" =>
        compact_refs([Map.get(state, :execution_outcome_ref), state.action_receipt.receipt_ref])
    }
    |> Map.new(fn {kind, refs} -> {kind, compact_refs(refs)} end)
    |> Map.reject(fn {_kind, refs} -> refs == [] end)
  end

  defp fixture_ref_set_complete?(receipt_ref_set) do
    required_kinds =
      ~w[session_refs turn_refs event_refs workspace_refs worker_refs authority_refs outcome_refs]

    Enum.all?(required_kinds, &(Map.get(receipt_ref_set, &1, []) != []))
  end

  defp compact_refs(refs), do: Enum.filter(refs, &present?/1)

  defp put_event(state, event_kind, summary) do
    event_seq = length(state.runtime_events) + 1

    event =
      RuntimeEventRow.new!(%{
        event_ref: "event://agent-loop/#{ref_suffix(state.run_ref)}/#{event_seq}",
        event_seq: event_seq,
        event_kind: event_kind,
        observed_at: @timestamp,
        tenant_ref: state.tenant_ref,
        installation_ref: state.installation_ref,
        subject_ref: state.subject_ref,
        run_ref: state.run_ref,
        workflow_ref: state.workflow_ref,
        turn_ref: state.turn_ref,
        level: "info",
        message_summary: summary,
        trace_id: state.trace_id,
        profile_ref: state.profile_ref,
        source_contract_ref: "agent-loop.v1"
      })

    Map.update!(state, :runtime_events, &(&1 ++ [event]))
  end

  defp tool_ref_for("denied_write_then_allowed_read"), do: "fixture.write_note"
  defp tool_ref_for(_script), do: "fixture.record_note"

  defp capability_ref_for("denied_write_then_allowed_read"), do: "capability://fixture/write-note"
  defp capability_ref_for(_script), do: "capability://fixture/record-note"

  defp ref_suffix(ref) when is_binary(ref) do
    ref
    |> String.replace(~r/[^A-Za-z0-9]+/, "-")
    |> String.trim("-")
  end

  defp ref_suffix(ref), do: ref |> to_string() |> ref_suffix()

  defp normalize(input) do
    case Support.normalize_attrs(input) do
      {:ok, attrs} -> attrs
      {:error, _reason} -> %{}
    end
  end

  defp map_value(map, key, default \\ nil)

  defp map_value(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp present?(value), do: is_binary(value) and String.trim(value) != ""
end

defmodule Mezzanine.Workflows.AgentLoop do
  @moduledoc "Temporal workflow for the provider-neutral M2 AgentLoop."

  use Temporalex.Workflow, task_queue: "mezzanine.agentic"

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Workflow
  def run(input) do
    with {:ok, pinned} <-
           execute_activity(Mezzanine.Activities.AgentLoopWakeAndPin, input,
             task_queue: "mezzanine.agentic",
             start_to_close_timeout: :timer.seconds(10)
           ),
         {:ok, recalled} <-
           execute_activity(Mezzanine.Activities.AgentLoopRecall, pinned,
             task_queue: "mezzanine.semantic",
             start_to_close_timeout: :timer.seconds(10)
           ),
         {:ok, assembled} <-
           execute_activity(Mezzanine.Activities.AgentLoopAssembleContext, recalled,
             task_queue: "mezzanine.semantic",
             start_to_close_timeout: :timer.seconds(10)
           ),
         {:ok, reflected} <-
           execute_activity(Mezzanine.Activities.AgentLoopReflect, assembled,
             task_queue: "mezzanine.semantic",
             start_to_close_timeout: :timer.seconds(10)
           ),
         {:ok, governed} <-
           execute_activity(Mezzanine.Activities.AgentLoopGovern, reflected,
             task_queue: "mezzanine.agentic",
             start_to_close_timeout: :timer.seconds(10)
           ),
         {:ok, submitted} <-
           execute_activity(Mezzanine.Activities.AgentLoopSubmitLowerRun, governed,
             task_queue: "mezzanine.hazmat",
             start_to_close_timeout: :timer.seconds(30)
           ),
         {:ok, observed} <-
           execute_activity(Mezzanine.Activities.AgentLoopAwaitExecutionOutcome, submitted,
             task_queue: "mezzanine.hazmat",
             start_to_close_timeout: :timer.seconds(30)
           ),
         {:ok, semanticized} <-
           execute_activity(Mezzanine.Activities.AgentLoopSemanticizeOutcome, observed,
             task_queue: "mezzanine.semantic",
             start_to_close_timeout: :timer.seconds(10)
           ),
         {:ok, memory_checked} <-
           execute_activity(Mezzanine.Activities.AgentLoopCommitPrivateMemory, semanticized,
             task_queue: "mezzanine.agentic",
             start_to_close_timeout: :timer.seconds(10)
           ),
         {:ok, projection} <-
           execute_activity(Mezzanine.Activities.AgentLoopAdvanceTurn, memory_checked,
             task_queue: "mezzanine.agentic",
             start_to_close_timeout: :timer.seconds(10)
           ) do
      {:ok, readback} = AgentLoop.to_runtime_run_detail(projection)
      set_state(readback)
      {:ok, projection}
    end
  end

  @impl Temporalex.Workflow
  def handle_signal(signal_name, payload, state) do
    signal =
      payload
      |> normalize_signal_payload()
      |> Map.put(:signal_name, signal_name)

    case AgentLoop.apply_signal(state || AgentLoop.initial_state(%{}), signal) do
      {:ok, next_state} -> {:noreply, next_state}
      {:error, reason} -> {:noreply, Map.put(state || %{}, :last_signal_error, reason)}
    end
  end

  @impl Temporalex.Workflow
  def handle_query("agent_loop.readback.v1", _args, state), do: {:reply, state || %{}}

  def handle_query("operator_state.v1", _args, state), do: {:reply, state || %{}}

  def handle_query("status", _args, state), do: {:reply, state || %{}}

  defp normalize_signal_payload(payload) when is_map(payload), do: payload
  defp normalize_signal_payload(_payload), do: %{}
end

defmodule Mezzanine.Activities.AgentLoopWakeAndPin do
  @moduledoc "M2 activity that pins run identity and initializes turn budget."

  use Temporalex.Activity,
    task_queue: "mezzanine.agentic",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.wake_and_pin_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopRecall do
  @moduledoc "M2 semantic recall activity returning compact refs."

  use Temporalex.Activity,
    task_queue: "mezzanine.semantic",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.recall_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopAssembleContext do
  @moduledoc "M2 context-pack assembly activity returning refs only."

  use Temporalex.Activity,
    task_queue: "mezzanine.semantic",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.assemble_context_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopReflect do
  @moduledoc "M2 reflection activity that emits provider-neutral action requests."

  use Temporalex.Activity,
    task_queue: "mezzanine.semantic",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.reflect_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopGovern do
  @moduledoc "M2 per-action governance activity."

  use Temporalex.Activity,
    task_queue: "mezzanine.agentic",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.govern_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopSubmitLowerRun do
  @moduledoc "M2 lower-run submission activity behind the hazmat queue."

  use Temporalex.Activity,
    task_queue: "mezzanine.hazmat",
    start_to_close_timeout: 30_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.submit_lower_run_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopAwaitExecutionOutcome do
  @moduledoc "M2 activity that observes lower execution outcome refs."

  use Temporalex.Activity,
    task_queue: "mezzanine.hazmat",
    start_to_close_timeout: 30_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.await_execution_outcome_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopSemanticizeOutcome do
  @moduledoc "M2 semanticization activity that emits CandidateFact refs."

  use Temporalex.Activity,
    task_queue: "mezzanine.semantic",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.semanticize_outcome_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopCommitPrivateMemory do
  @moduledoc "M2 memory activity; Phase 5 records a skipped receipt posture."

  use Temporalex.Activity,
    task_queue: "mezzanine.agentic",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.commit_private_memory_activity(input)
end

defmodule Mezzanine.Activities.AgentLoopAdvanceTurn do
  @moduledoc "M2 activity that advances the turn and publishes M1-readable projection rows."

  use Temporalex.Activity,
    task_queue: "mezzanine.agentic",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 3]

  alias Mezzanine.WorkflowRuntime.AgentLoop

  @impl Temporalex.Activity
  def perform(input), do: AgentLoop.advance_turn_activity(input)
end
