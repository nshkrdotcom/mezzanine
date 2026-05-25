defmodule Mezzanine.CoordinationEngine.StateMachine do
  @moduledoc """
  Ref-only coordination lifecycle transitions.
  """

  alias Mezzanine.CoordinationEngine.{Run, Validation, VerifierPolicy}

  @allowed_transitions %{
    created: [:router_ready, :cancelled, :failed, :replaced],
    router_ready: [:provider_pool_ready, :cancelled, :failed, :replaced],
    provider_pool_ready: [:routing, :cancelled, :failed, :replaced],
    routing: [:role_injected, :cancelled, :failed, :replaced],
    role_injected: [:agent_turn_started, :cancelled, :failed, :replaced],
    agent_turn_started: [:agent_turn_completed, :cancelled, :failed, :replaced],
    agent_turn_completed: [
      :verifier_running,
      :handoff_requested,
      :terminated,
      :cancelled,
      :failed,
      :replaced
    ],
    verifier_running: [:agent_turn_completed, :terminated, :cancelled, :failed, :replaced],
    handoff_requested: [:handoff_accepted, :cancelled, :failed, :replaced],
    handoff_accepted: [:agent_turn_started, :terminated, :cancelled, :failed, :replaced],
    terminated: [],
    cancelled: [],
    failed: [],
    replaced: []
  }

  @router_ready_refs [:router_artifact_ref, :extractor_ref, :head_ref, :trace_ref]
  @inject_role_refs [
    :role_ref,
    :prompt_ref,
    :memory_ref_set,
    :context_budget_ref,
    :handoff_policy_ref,
    :appkit_projection_ref
  ]
  @start_turn_refs [
    :turn_ref,
    :agent_ref,
    :role_ref,
    :router_decision_ref,
    :provider_pool_ref,
    :model_profile_ref,
    :endpoint_profile_ref,
    :inference_call_ref,
    :memory_ref_set,
    :context_budget_ref,
    :cost_budget_ref,
    :trace_ref,
    :replay_ref,
    :operator_action_ref,
    :handoff_policy_ref
  ]
  @complete_turn_refs [
    :turn_ref,
    :inference_call_ref,
    :memory_ref_set,
    :context_budget_ref,
    :cost_budget_ref,
    :trace_ref,
    :replay_ref
  ]
  @handoff_request_refs [
    :handoff_ref,
    :from_role_ref,
    :to_role_ref,
    :memory_summary_ref,
    :trace_ref,
    :replay_ref
  ]
  @handoff_accept_refs [:handoff_ref, :accepting_agent_ref, :authority_ref, :trace_ref]
  @termination_refs [:termination_ref, :verifier_result_ref, :replay_ref, :trace_ref]
  @cancel_refs [:cancellation_ref, :authority_ref, :trace_ref]
  @fail_refs [:failure_ref, :trace_ref]
  @replace_refs [:replacement_ref, :authority_ref, :trace_ref]

  @spec router_ready(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def router_ready(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @router_ready_refs) do
      transition(run, :router_ready, attrs)
    end
  end

  @spec provider_pool_ready(Run.t(), [map()] | map()) :: {:ok, Run.t()} | {:error, term()}
  def provider_pool_ready(%Run{} = run, slots) when is_list(slots) do
    with {:ok, slot_refs} <- provider_slot_refs(slots) do
      transition(run, :provider_pool_ready, %{
        provider_pool_ref: run.spec.provider_pool_ref,
        provider_slot_refs: slot_refs,
        trace_ref: "trace:provider-pool-ready:" <> run.spec.coordination_run_ref
      })
    end
  end

  def provider_pool_ready(%Run{} = run, attrs) when is_map(attrs) do
    with :ok <- require_refs(attrs, [:provider_pool_ref, :trace_ref]) do
      transition(run, :provider_pool_ready, attrs)
    end
  end

  @spec route(Run.t(), map(), map()) :: {:ok, Run.t(), map()} | {:error, term()}
  def route(%Run{} = run, config, attrs) do
    with {:ok, decision} <- route_decision(run, config, attrs),
         {:ok, next_run} <- transition(run, :routing, attrs) do
      {:ok,
       %{
         next_run
         | router_decision_ref: decision.router_decision_ref,
           selected_role_ref: decision.selected_role_ref,
           trace_refs: append_ref(next_run.trace_refs, decision.trace_ref),
           replay_refs: append_ref(next_run.replay_refs, decision.replay_ref)
       }, decision}
    end
  end

  defp provider_slot_refs(slots) do
    slot_refs =
      slots
      |> Enum.map(&Validation.fetch(&1, :slot_ref))
      |> Enum.filter(&(is_binary(&1) and &1 != ""))

    if slot_refs == [] do
      {:error, {:missing_required_ref, :slot_ref}}
    else
      {:ok, slot_refs}
    end
  end

  defp route_decision(run, _config, attrs) do
    with {:ok, selected_role_ref} <- Validation.require_binary(attrs, :preferred_role_ref),
         {:ok, trace_ref} <- Validation.require_binary(attrs, :trace_ref),
         {:ok, replay_ref} <- Validation.require_binary(attrs, :replay_ref) do
      {:ok,
       %{
         router_decision_ref:
           "router_decision:#{run.spec.coordination_run_ref}:#{selected_role_ref}",
         selected_role_ref: selected_role_ref,
         trace_ref: trace_ref,
         replay_ref: replay_ref
       }}
    end
  end

  @spec inject_role(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def inject_role(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @inject_role_refs) do
      transition(run, :role_injected, attrs)
    end
  end

  @spec start_turn(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def start_turn(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @start_turn_refs),
         {:ok, next_run} <- transition(run, :agent_turn_started, attrs) do
      {:ok, %{next_run | current_turn_ref: Validation.fetch(attrs, :turn_ref)}}
    end
  end

  @spec complete_turn(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def complete_turn(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @complete_turn_refs) do
      transition(run, :agent_turn_completed, attrs)
    end
  end

  @spec run_verifier(Run.t(), map()) :: {:ok, Run.t(), VerifierPolicy.t()} | {:error, term()}
  def run_verifier(%Run{} = run, attrs) do
    with {:ok, policy} <- VerifierPolicy.new(attrs),
         {:ok, next_run} <- transition(run, :verifier_running, attrs) do
      {:ok, %{next_run | verifier_policy: policy}, policy}
    end
  end

  @spec complete_verifier(Run.t(), map()) ::
          {:ok, Run.t(), VerifierPolicy.Decision.t()} | {:error, term()}
  def complete_verifier(%Run{} = run, attrs) do
    with {:ok, verifier_result_ref} <- Validation.require_binary(attrs, :verifier_result_ref),
         {:ok, policy} <- verifier_policy(run, attrs),
         {:ok, decision} <- VerifierPolicy.evaluate(policy, attrs),
         {:ok, next_run} <- transition(run, :agent_turn_completed, attrs) do
      {:ok, %{next_run | verifier_result_ref: verifier_result_ref}, decision}
    end
  end

  @spec request_handoff(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def request_handoff(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @handoff_request_refs),
         {:ok, next_run} <- transition(run, :handoff_requested, attrs) do
      {:ok, %{next_run | handoff_ref: Validation.fetch(attrs, :handoff_ref)}}
    end
  end

  @spec accept_handoff(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def accept_handoff(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @handoff_accept_refs) do
      transition(run, :handoff_accepted, attrs)
    end
  end

  @spec terminate(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def terminate(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @termination_refs),
         {:ok, next_run} <- transition(run, :terminated, attrs) do
      {:ok, %{next_run | termination_ref: Validation.fetch(attrs, :termination_ref)}}
    end
  end

  @spec cancel(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def cancel(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @cancel_refs),
         {:ok, next_run} <- transition(run, :cancelled, attrs) do
      {:ok, %{next_run | cancellation_ref: Validation.fetch(attrs, :cancellation_ref)}}
    end
  end

  @spec fail(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def fail(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @fail_refs),
         {:ok, next_run} <- transition(run, :failed, attrs) do
      {:ok, %{next_run | failure_ref: Validation.fetch(attrs, :failure_ref)}}
    end
  end

  @spec replace(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def replace(%Run{} = run, attrs) do
    with :ok <- require_refs(attrs, @replace_refs),
         {:ok, next_run} <- transition(run, :replaced, attrs) do
      {:ok, %{next_run | replacement_ref: Validation.fetch(attrs, :replacement_ref)}}
    end
  end

  defp verifier_policy(%Run{verifier_policy: %VerifierPolicy{} = policy}, _attrs),
    do: {:ok, policy}

  defp verifier_policy(%Run{}, attrs), do: VerifierPolicy.new(attrs)

  defp transition(%Run{} = run, next_state, attrs) when is_map(attrs) do
    with :ok <- Validation.reject_raw(attrs),
         :ok <- allowed_transition(run.state, next_state) do
      {:ok,
       %{
         run
         | state: next_state,
           state_history: run.state_history ++ [next_state],
           trace_refs: append_ref(run.trace_refs, Validation.fetch(attrs, :trace_ref)),
           replay_refs: append_ref(run.replay_refs, Validation.fetch(attrs, :replay_ref))
       }}
    end
  end

  defp allowed_transition(state, next_state) do
    allowed_states = Map.fetch!(@allowed_transitions, state)

    if next_state in allowed_states do
      :ok
    else
      {:error, {:invalid_coordination_transition, state, next_state}}
    end
  end

  defp require_refs(attrs, keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      case require_ref(attrs, key) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp require_ref(attrs, key) when key in [:memory_ref_set] do
    case Validation.require_string_list(attrs, key) do
      {:ok, _refs} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp require_ref(attrs, key) do
    case Validation.require_binary(attrs, key) do
      {:ok, _ref} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp append_ref(refs, ref) when is_binary(ref) and ref != "", do: refs ++ [ref]
  defp append_ref(refs, _ref), do: refs
end
