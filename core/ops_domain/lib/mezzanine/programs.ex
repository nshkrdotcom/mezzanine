defmodule Mezzanine.Programs do
  @moduledoc """
  Durable program and environment-facing truth.
  """

  use Ash.Domain, validate_config_inclusion?: false

  resources do
    resource Mezzanine.Programs.Program
    resource Mezzanine.Programs.PolicyBundle
    resource Mezzanine.Programs.PlacementProfile
  end
end
