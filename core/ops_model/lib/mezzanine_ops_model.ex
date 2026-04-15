defmodule MezzanineOpsModel do
  @moduledoc """
  Pure operational vocabulary for Mezzanine.

  This package holds data-only semantic structs, state vocabularies, and
  normalization helpers that higher layers can share without pulling in Ash,
  OTP lifecycle, or lower-stack runtime code.
  """

  @typedoc "Opaque identifier used across the pure operational model."
  @type id :: String.t()
end
