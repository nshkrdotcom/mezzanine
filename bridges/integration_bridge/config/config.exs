import Config

config :ash,
  domains: [Mezzanine.Audit]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.Audit]

config :mezzanine_integration_bridge,
  ecto_repos: [Mezzanine.Audit.Repo]

import_config "#{config_env()}.exs"
