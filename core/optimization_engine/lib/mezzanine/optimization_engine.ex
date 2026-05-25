defmodule Mezzanine.OptimizationEngine do
  @moduledoc """
  Governed GEPA optimization orchestration facade.
  """

  alias Mezzanine.AIExecution
  alias Mezzanine.OptimizationEngine.{PriorFabric, RunSpec}

  @spec prepare_run(map()) :: {:ok, RunSpec.t()} | {:error, term()}
  def prepare_run(attrs) when is_map(attrs), do: RunSpec.new(attrs)
  def prepare_run(_attrs), do: {:error, :invalid_optimization_run_spec}

  @spec bind_prior_fabric(map()) :: {:ok, PriorFabric.Receipt.t()} | {:error, term()}
  def bind_prior_fabric(attrs), do: PriorFabric.bind(attrs)

  @spec propose_candidates(map(), AIExecution.RuntimeDeps.t() | map() | keyword(), keyword()) ::
          {:ok, [AIExecution.OptimizerAdapter.candidate_receipt()]} | {:error, term()}
  def propose_candidates(attrs, runtime_deps \\ %AIExecution.RuntimeDeps{}, opts \\ [])

  def propose_candidates(attrs, runtime_deps, opts) when is_map(attrs) and is_list(opts) do
    with {:ok, spec} <- prepare_run(attrs) do
      spec
      |> RunSpec.optimizer_request()
      |> AIExecution.propose(runtime_deps, opts)
    end
  end

  def propose_candidates(_attrs, _runtime_deps, _opts),
    do: {:error, :invalid_optimization_run_spec}
end
