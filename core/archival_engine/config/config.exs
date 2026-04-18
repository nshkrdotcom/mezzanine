import Config

config :ash,
  domains: [
    Mezzanine.Archival,
    Mezzanine.ConfigRegistry,
    Mezzanine.EvidenceLedger,
    Mezzanine.Decisions,
    Mezzanine.Execution,
    Mezzanine.Objects,
    Mezzanine.Audit
  ]

config :mezzanine_archival_engine,
  ecto_repos: [Mezzanine.Archival.Repo],
  ash_domains: [Mezzanine.Archival],
  start_runtime_children?: true,
  cold_store: [
    module: Mezzanine.Archival.FileSystemColdStore,
    root: Path.expand("../tmp/archival_store", __DIR__)
  ],
  scheduler: [
    enabled?: false,
    interval_ms: :timer.minutes(5)
  ]

config :mezzanine_config_registry,
  ecto_repos: [Mezzanine.ConfigRegistry.Repo],
  ash_domains: [Mezzanine.ConfigRegistry]

config :mezzanine_evidence_engine,
  ecto_repos: [Mezzanine.EvidenceLedger.Repo],
  ash_domains: [Mezzanine.EvidenceLedger]

config :mezzanine_decision_engine,
  ecto_repos: [Mezzanine.Decisions.Repo],
  ash_domains: [Mezzanine.Decisions]

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

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

import_config "#{config_env()}.exs"
