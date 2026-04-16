defmodule Mezzanine.Decisions do
  @moduledoc """
  Neutral Ash domain for substrate-owned decision and review truth.
  """

  use Ash.Domain

  resources do
    resource(Mezzanine.Decisions.DecisionRecord)
  end
end
