import Config

config :mezzanine_archival_engine, Mezzanine.Archival.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_archival_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2

config :mezzanine_evidence_engine, Mezzanine.EvidenceLedger.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_archival_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2

config :mezzanine_decision_engine, Mezzanine.Decisions.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_archival_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2

config :mezzanine_execution_engine, Mezzanine.Execution.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_archival_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2

config :mezzanine_object_engine, Mezzanine.Objects.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_archival_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2

config :mezzanine_audit_engine, Mezzanine.Audit.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_archival_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2
