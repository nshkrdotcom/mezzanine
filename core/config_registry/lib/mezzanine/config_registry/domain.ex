defmodule Mezzanine.ConfigRegistry do
  @moduledoc false
  use Ash.Domain

  resources do
    resource(Mezzanine.ConfigRegistry.PackRegistration)
    resource(Mezzanine.ConfigRegistry.Installation)
    resource(Mezzanine.ConfigRegistry.Policy)
  end
end
