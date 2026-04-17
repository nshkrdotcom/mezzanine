import Config

config :ash,
  domains: [
    Mezzanine.Audit,
    Mezzanine.Objects,
    Mezzanine.Execution,
    Mezzanine.Programs,
    Mezzanine.Work,
    Mezzanine.Runs,
    Mezzanine.Review,
    Mezzanine.Evidence,
    Mezzanine.Control
  ]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.Audit]

config :mezzanine_object_engine,
  ecto_repos: [Mezzanine.Objects.Repo],
  ash_domains: [Mezzanine.Objects]

config :mezzanine_execution_engine,
  ecto_repos: [Mezzanine.Execution.Repo],
  ash_domains: [Mezzanine.Execution]

config :mezzanine_ops_domain,
  ecto_repos: [Mezzanine.OpsDomain.Repo],
  ash_domains: [
    Mezzanine.Programs,
    Mezzanine.Work,
    Mezzanine.Runs,
    Mezzanine.Review,
    Mezzanine.Evidence,
    Mezzanine.Control
  ]

import_config "#{config_env()}.exs"
