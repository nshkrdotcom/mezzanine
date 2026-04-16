import Config

config :mezzanine_core,
  ash_domains: [
    Mezzanine.ConfigRegistry,
    Mezzanine.Audit,
    Mezzanine.Objects,
    Mezzanine.Execution,
    Mezzanine.Decisions,
    Mezzanine.EvidenceLedger,
    Mezzanine.Projections,
    Mezzanine.Archival
  ]
