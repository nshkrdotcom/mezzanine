defmodule MezzanineArchivalEngine do
  @moduledoc """
  Durable archival scheduler, cold-store, and manifest surface for the neutral
  Mezzanine runtime.
  """

  @spec contract_modules() :: [module()]
  def contract_modules do
    [
      Mezzanine.Archival.ArchivalManifest,
      Mezzanine.Archival.ColdStore,
      Mezzanine.Archival.FileSystemColdStore,
      Mezzanine.Archival.Query,
      Mezzanine.Archival.Scheduler,
      Mezzanine.Archival.Snapshot
    ]
  end
end
