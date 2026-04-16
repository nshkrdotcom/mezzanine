defmodule Mezzanine.Pack do
  @moduledoc """
  Behaviour implemented by neutral domain packs.
  """

  @callback manifest() :: Mezzanine.Pack.Manifest.t()
end
