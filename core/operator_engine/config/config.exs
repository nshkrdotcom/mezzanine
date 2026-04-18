import Config

config :ash,
  domains: [
    Mezzanine.Execution,
    Mezzanine.Objects,
    Mezzanine.Audit
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
  queues: [dispatch: 10, receipt: 10, reconcile: 10, cancel: 10, decision_expiry: 10],
  plugins: []

config :mezzanine_object_engine,
  ecto_repos: [Mezzanine.Objects.Repo],
  ash_domains: [Mezzanine.Objects]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.Audit]

config :mezzanine_operator_engine,
  ecto_repos: [
    Mezzanine.Execution.Repo,
    Mezzanine.Objects.Repo,
    Mezzanine.Audit.Repo
  ],
  ash_domains: [
    Mezzanine.Execution,
    Mezzanine.Objects,
    Mezzanine.Audit
  ]

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

import_config "#{config_env()}.exs"
