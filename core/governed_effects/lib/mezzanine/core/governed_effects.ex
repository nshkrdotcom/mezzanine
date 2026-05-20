defmodule Mezzanine.Core.GovernedEffects do
  @moduledoc """
  Pure governed-effect lifecycle contracts.

  This package deliberately owns values and pure lifecycle checks only.
  Projection reducers, dispatch, and authority calls are implemented in later
  phases by their owning Mezzanine packages.
  """
end
