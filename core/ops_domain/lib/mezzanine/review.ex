defmodule Mezzanine.Review do
  @moduledoc """
  Durable review, waiver, and escalation truth for governed work.
  """

  use Ash.Domain, validate_config_inclusion?: false

  resources do
    resource Mezzanine.Review.ReviewUnit
    resource Mezzanine.Review.ReviewDecision
    resource Mezzanine.Review.Waiver
    resource Mezzanine.Review.Escalation
  end
end
