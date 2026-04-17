import Config

config :ash,
  domains: [Mezzanine.Audit]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.Audit]

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

config :mezzanine_integration_bridge,
  ecto_repos: [Mezzanine.Audit.Repo, Mezzanine.OpsDomain.Repo]

import_config "#{config_env()}.exs"
