defmodule MezzanineProjectionEngine do
  @moduledoc """
  Durable named projection rows and async materialized projections for the
  neutral Mezzanine runtime.
  """

  @spec domain_modules() :: [module()]
  def domain_modules do
    [Mezzanine.Projections]
  end

  @spec resource_modules() :: [module()]
  def resource_modules do
    [
      Mezzanine.Projections.ProjectionRow,
      Mezzanine.Projections.MaterializedProjection
    ]
  end

  @spec service_modules() :: [module()]
  def service_modules do
    [
      Mezzanine.Projections.ReceiptReducer,
      Mezzanine.Projections.ReviewGate,
      Mezzanine.Projections.SourceReconciliation
    ]
  end
end
