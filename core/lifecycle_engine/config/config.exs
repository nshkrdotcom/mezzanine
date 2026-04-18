import Config

config :ash,
  domains: [
    Mezzanine.Execution,
    Mezzanine.Objects,
    Mezzanine.Audit,
    Mezzanine.ConfigRegistry
  ]

config :mezzanine_execution_engine,
  ecto_repos: [Mezzanine.Execution.Repo],
  ash_domains: [Mezzanine.Execution]

config :mezzanine_execution_engine, Oban,
  name: Mezzanine.Execution.Oban,
  repo: Mezzanine.Execution.Repo,
  engine: Oban.Engines.Basic,
  notifier: Oban.Notifiers.Postgres,
  peer: false,
  queues: [dispatch: 10],
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

config :mezzanine_lifecycle_engine,
  ecto_repos: [
    Mezzanine.Execution.Repo,
    Mezzanine.Objects.Repo,
    Mezzanine.Audit.Repo,
    Mezzanine.ConfigRegistry.Repo
  ],
  ash_domains: [
    Mezzanine.Execution,
    Mezzanine.Objects,
    Mezzanine.Audit,
    Mezzanine.ConfigRegistry
  ]

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

import_config "#{config_env()}.exs"
