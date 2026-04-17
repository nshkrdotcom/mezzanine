defmodule Mezzanine.Planner do
  @moduledoc """
  Public pure-planning facade.
  """

  alias Mezzanine.Planner.{DependencyResolver, ObligationDeriver, PlanCompiler, RetryScheduler}
  alias MezzanineOpsModel.{PolicyBundle, WorkObject, WorkPlan}

  @spec compile(WorkObject.t(), PolicyBundle.t()) :: {:ok, WorkPlan.t()} | {:error, term()}
  def compile(%WorkObject{} = work, %PolicyBundle{} = bundle) do
    PlanCompiler.compile(work, bundle)
  end

  @spec derive_obligations(WorkPlan.t()) :: [MezzanineOpsModel.Obligation.t()]
  def derive_obligations(%WorkPlan{} = plan) do
    ObligationDeriver.derive(plan)
  end

  @spec next_retry(map(), map(), DateTime.t()) :: {:ok, map()} | {:error, :retry_exhausted}
  def next_retry(failure_state, retry_profile, now \\ DateTime.utc_now()) do
    RetryScheduler.next_retry(failure_state, retry_profile, now)
  end

  @spec ready_to_plan?(WorkObject.t()) :: boolean()
  def ready_to_plan?(%WorkObject{} = work) do
    DependencyResolver.ready_to_plan?(work)
  end
end
