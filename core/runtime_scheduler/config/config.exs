import Config

config :ash,
  domains: [Mezzanine.Execution, Mezzanine.Objects, Mezzanine.Audit, Mezzanine.ConfigRegistry]

config :mezzanine_execution_engine,
  ecto_repos: [Mezzanine.Execution.Repo],
  ash_domains: [Mezzanine.Execution]

config :mezzanine_execution_engine, Oban,
  name: Mezzanine.Execution.Oban,
  repo: Mezzanine.Execution.Repo,
  engine: Oban.Engines.Basic,
  notifier: Oban.Notifiers.Postgres,
  peer: false,
  queues: [workflow_start_outbox: 10, workflow_signal_outbox: 10, claim_check_gc: 2],
  plugins: []

config :mezzanine_object_engine,
  ecto_repos: [Mezzanine.Objects.Repo],
  ash_domains: [Mezzanine.Objects]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.Audit]

config :mezzanine_config_registry,
  ecto_repos: [Mezzanine.ConfigRegistry.Repo],
  ash_domains: [Mezzanine.ConfigRegistry]

config :mezzanine_runtime_scheduler,
  ecto_repos: [
    Mezzanine.RuntimeScheduler.Repo,
    Mezzanine.Execution.Repo,
    Mezzanine.Objects.Repo,
    Mezzanine.Audit.Repo,
    Mezzanine.ConfigRegistry.Repo
  ],
  ash_domains: [Mezzanine.Execution, Mezzanine.Objects, Mezzanine.Audit, Mezzanine.ConfigRegistry]

import_config "#{config_env()}.exs"
