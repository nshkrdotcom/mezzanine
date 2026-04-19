import Config

config :mezzanine_evidence_engine, Mezzanine.EvidenceLedger.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_evidence_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_execution_engine, Mezzanine.Execution.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_evidence_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_object_engine, Mezzanine.Objects.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_evidence_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_audit_engine, Mezzanine.Audit.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_evidence_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_ops_domain, Mezzanine.OpsDomain.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_evidence_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_execution_engine, Oban,
  name: Mezzanine.Execution.Oban,
  repo: Mezzanine.Execution.Repo,
  engine: Oban.Engines.Basic,
  notifier: Oban.Notifiers.Postgres,
  peer: false,
  queues: [workflow_start_outbox: 10, workflow_signal_outbox: 10, claim_check_gc: 2],
  plugins: []
