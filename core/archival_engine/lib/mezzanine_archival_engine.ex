defmodule MezzanineArchivalEngine do
  @moduledoc """
  Neutral archival-contract entrypoint for the Mezzanine rebuild.
  """

  @spec contract_modules() :: [module()]
  def contract_modules do
    [
      Mezzanine.Archival.CountdownPolicy,
      Mezzanine.Archival.Graph,
      Mezzanine.Archival.Manifest,
      Mezzanine.Archival.OffloadPlan
    ]
  end
end
