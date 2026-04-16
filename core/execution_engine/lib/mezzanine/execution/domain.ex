defmodule Mezzanine.Execution do
  @moduledoc """
  Neutral Ash domain for substrate execution and dispatch outbox truth.
  """

  use Ash.Domain

  resources do
    resource(Mezzanine.Execution.ExecutionRecord)
    resource(Mezzanine.Execution.DispatchOutboxEntry)
  end
end
