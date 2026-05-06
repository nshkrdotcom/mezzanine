defmodule Mezzanine.OptimizationEngine do
  @moduledoc """
  Governed GEPA optimization orchestration facade.
  """

  alias Mezzanine.OptimizationEngine.{PriorFabric, RunSpec}

  @spec prepare_run(map()) :: {:ok, RunSpec.t()} | {:error, term()}
  def prepare_run(attrs) when is_map(attrs), do: RunSpec.new(attrs)
  def prepare_run(_attrs), do: {:error, :invalid_optimization_run_spec}

  @spec bind_prior_fabric(map()) :: {:ok, PriorFabric.Receipt.t()} | {:error, term()}
  def bind_prior_fabric(attrs), do: PriorFabric.bind(attrs)
end
