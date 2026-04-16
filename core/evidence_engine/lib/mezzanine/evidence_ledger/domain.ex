defmodule Mezzanine.EvidenceLedger do
  @moduledoc """
  Neutral Ash domain for substrate-owned evidence collection truth.
  """

  use Ash.Domain

  resources do
    resource(Mezzanine.EvidenceLedger.EvidenceRecord)
  end
end
