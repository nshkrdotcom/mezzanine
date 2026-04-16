defmodule Mezzanine.ConfigRegistry do
  @moduledoc false
  use Ash.Domain

  resources do
    resource(Mezzanine.ConfigRegistry.PackRegistration)
    resource(Mezzanine.ConfigRegistry.Installation)
  end
end
