defmodule Mezzanine.CoordinationEngine do
  @moduledoc """
  Governed TRINITY coordination orchestration facade.
  """

  alias JidoHive.{CoordinationPatterns, InterAgentMessaging}
  alias Mezzanine.CoordinationEngine.{Run, RunSpec, StateMachine, TraceDataset, VerifierPolicy}

  @spec admit(map()) :: {:ok, Run.t()} | {:error, term()}
  def admit(attrs) when is_map(attrs) do
    with {:ok, spec} <- RunSpec.new(attrs) do
      Run.new(spec)
    end
  end

  def admit(_attrs), do: {:error, :invalid_coordination_run}

  @spec router_ready(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def router_ready(%Run{} = run, attrs), do: StateMachine.router_ready(run, attrs)

  @spec provider_pool_ready(Run.t(), [map()] | map()) :: {:ok, Run.t()} | {:error, term()}
  def provider_pool_ready(%Run{} = run, attrs), do: StateMachine.provider_pool_ready(run, attrs)

  @spec route(Run.t(), map(), map()) :: {:ok, Run.t(), map()} | {:error, term()}
  def route(%Run{} = run, config, attrs), do: StateMachine.route(run, config, attrs)

  @spec inject_role(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def inject_role(%Run{} = run, attrs), do: StateMachine.inject_role(run, attrs)

  @spec start_turn(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def start_turn(%Run{} = run, attrs), do: StateMachine.start_turn(run, attrs)

  @spec complete_turn(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def complete_turn(%Run{} = run, attrs), do: StateMachine.complete_turn(run, attrs)

  @spec run_verifier(Run.t(), map()) :: {:ok, Run.t(), VerifierPolicy.t()} | {:error, term()}
  def run_verifier(%Run{} = run, attrs), do: StateMachine.run_verifier(run, attrs)

  @spec complete_verifier(Run.t(), map()) ::
          {:ok, Run.t(), VerifierPolicy.Decision.t()} | {:error, term()}
  def complete_verifier(%Run{} = run, attrs), do: StateMachine.complete_verifier(run, attrs)

  @spec request_handoff(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def request_handoff(%Run{} = run, attrs), do: StateMachine.request_handoff(run, attrs)

  @spec accept_handoff(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def accept_handoff(%Run{} = run, attrs), do: StateMachine.accept_handoff(run, attrs)

  @spec terminate(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def terminate(%Run{} = run, attrs), do: StateMachine.terminate(run, attrs)

  @spec cancel(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def cancel(%Run{} = run, attrs), do: StateMachine.cancel(run, attrs)

  @spec fail(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def fail(%Run{} = run, attrs), do: StateMachine.fail(run, attrs)

  @spec replace(Run.t(), map()) :: {:ok, Run.t()} | {:error, term()}
  def replace(%Run{} = run, attrs), do: StateMachine.replace(run, attrs)

  @spec plan_pattern(map()) :: {:ok, map()} | {:error, term()}
  def plan_pattern(attrs), do: CoordinationPatterns.plan(attrs)

  @spec route_message(map(), map()) ::
          {:ok, InterAgentMessaging.RoutedMessage.t()} | {:error, term()}
  def route_message(attrs, routing_context \\ %{}),
    do: InterAgentMessaging.route(attrs, routing_context)

  @spec trace_dataset(Run.t(), map()) :: {:ok, TraceDataset.Receipt.t()} | {:error, term()}
  def trace_dataset(%Run{} = run, attrs), do: TraceDataset.from_run(run, attrs)
end
