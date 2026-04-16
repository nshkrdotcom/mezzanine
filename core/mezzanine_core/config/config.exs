import Config

config :mezzanine_core,
  ash_domains: [
    Mezzanine.ConfigRegistry,
    Mezzanine.Audit,
    Mezzanine.Objects,
    Mezzanine.Execution
  ]
