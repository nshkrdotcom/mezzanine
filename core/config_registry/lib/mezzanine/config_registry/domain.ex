defmodule Mezzanine.ConfigRegistry do
  @moduledoc false
  use Ash.Domain

  resources do
    resource(Mezzanine.ConfigRegistry.ActiveBindingSet)
    resource(Mezzanine.ConfigRegistry.BindingManifestDependency)
    resource(Mezzanine.ConfigRegistry.BindingSet)
    resource(Mezzanine.ConfigRegistry.CompiledBinding)
    resource(Mezzanine.ConfigRegistry.PackRegistration)
    resource(Mezzanine.ConfigRegistry.Installation)
    resource(Mezzanine.ConfigRegistry.Policy)
    resource(Mezzanine.ConfigRegistry.RunBindingSnapshot)
  end
end
