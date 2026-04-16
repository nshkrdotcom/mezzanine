import Config

config :mezzanine_execution_engine, Mezzanine.Execution.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_execution_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10

config :mezzanine_object_engine, Mezzanine.Objects.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_execution_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10

config :mezzanine_audit_engine, Mezzanine.Audit.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_execution_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10
