defmodule Mezzanine.Projections do
  @moduledoc """
  Neutral Ash domain for durable named projection rows and async materializations.
  """

  use Ash.Domain

  resources do
    resource(Mezzanine.Projections.ProjectionRow)
    resource(Mezzanine.Projections.MaterializedProjection)
  end
end
