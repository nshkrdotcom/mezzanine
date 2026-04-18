defmodule MezzanineLeasing do
  @moduledoc """
  Public entrypoint for the Mezzanine leasing package.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.Leasing,
      Mezzanine.Leasing.Repo,
      Mezzanine.ReadLease,
      Mezzanine.StreamAttachLease,
      Mezzanine.LeaseInvalidation,
      Mezzanine.StreamAttachHost
    ]
  end
end
