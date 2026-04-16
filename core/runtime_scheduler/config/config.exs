import Config

config :ash,
  domains: [Mezzanine.Execution, Mezzanine.Objects, Mezzanine.AuditDomain]

config :mezzanine_execution_engine,
  ecto_repos: [Mezzanine.Execution.Repo],
  ash_domains: [Mezzanine.Execution]

config :mezzanine_object_engine,
  ecto_repos: [Mezzanine.Objects.Repo],
  ash_domains: [Mezzanine.Objects]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.AuditDomain]

config :mezzanine_runtime_scheduler,
  ecto_repos: [
    Mezzanine.RuntimeScheduler.Repo,
    Mezzanine.Execution.Repo,
    Mezzanine.Objects.Repo,
    Mezzanine.Audit.Repo
  ],
  ash_domains: [Mezzanine.Execution, Mezzanine.Objects, Mezzanine.AuditDomain]

import_config "#{config_env()}.exs"
