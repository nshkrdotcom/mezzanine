import Config

config :ash,
  domains: [Mezzanine.AuditDomain]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.AuditDomain]

config :mezzanine_integration_bridge,
  ecto_repos: [Mezzanine.Audit.Repo]

import_config "#{config_env()}.exs"
