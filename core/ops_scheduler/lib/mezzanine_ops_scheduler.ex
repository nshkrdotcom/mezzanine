defmodule MezzanineOpsScheduler do
  @moduledoc """
  Internal scheduler runtime package for Mezzanine.

  This package owns the first process-bearing runtime slice above the durable
  Ash domains:

  - tick ownership
  - due-work selection
  - lease claims
  - concurrency checks
  - retry eligibility
  - stall detection
  - restart reconciliation
  """
end
