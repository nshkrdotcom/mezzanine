defmodule MezzanineArchivalEngine do
  @moduledoc """
  Durable archival manifest persistence plus pure countdown/offload contracts for
  the neutral Mezzanine runtime.
  """

  @spec contract_modules() :: [module()]
  def contract_modules do
    [
      Mezzanine.Archival.CountdownPolicy,
      Mezzanine.Archival.Graph,
      Mezzanine.Archival.Manifest,
      Mezzanine.Archival.OffloadPlan,
      Mezzanine.Archival.ArchivalManifest
    ]
  end
end
