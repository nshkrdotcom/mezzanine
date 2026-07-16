import Config

config :mezzanine_core,
  ecto_repos: [Mezzanine.Repo],
  run_store: Mezzanine.WorkflowRuntime.Store.Postgres,
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

import_config "#{config_env()}.exs"
