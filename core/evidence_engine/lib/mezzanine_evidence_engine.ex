defmodule MezzanineEvidenceEngine do
  @moduledoc """
  Neutral evidence-ledger contract entrypoint for the Mezzanine rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.EvidenceLedger,
      Mezzanine.EvidenceLedger.EvidenceRecord,
      Mezzanine.EvidenceLedger.Summary
    ]
  end
end
