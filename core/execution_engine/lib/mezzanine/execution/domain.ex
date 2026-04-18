defmodule Mezzanine.Execution do
  @moduledoc """
  Neutral Ash domain for substrate execution truth.
  """

  use Ash.Domain

  resources do
    resource(Mezzanine.Execution.ExecutionRecord)
  end
end
