defmodule Mezzanine.Archival do
  @moduledoc """
  Neutral Ash domain for durable archival manifests and offload metadata.
  """

  use Ash.Domain

  resources do
    resource(Mezzanine.Archival.ArchivalManifest)
  end
end
