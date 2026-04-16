defmodule Mezzanine.Objects do
  @moduledoc """
  Neutral Ash domain for substrate-owned subject ledger state.
  """

  use Ash.Domain

  resources do
    resource(Mezzanine.Objects.SubjectRecord)
  end
end
