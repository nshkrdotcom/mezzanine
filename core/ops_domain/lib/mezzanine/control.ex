defmodule Mezzanine.Control do
  @moduledoc """
  Durable operator control truth for governed work.
  """

  use Ash.Domain, validate_config_inclusion?: false

  resources do
    resource Mezzanine.Control.ControlSession
    resource Mezzanine.Control.OperatorIntervention
  end
end
