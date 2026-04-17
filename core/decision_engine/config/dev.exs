import Config

config :mezzanine_decision_engine, Mezzanine.Decisions.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_decision_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_execution_engine, Mezzanine.Execution.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_decision_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_object_engine, Mezzanine.Objects.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_decision_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_audit_engine, Mezzanine.Audit.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_decision_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_ops_domain, Mezzanine.OpsDomain.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_decision_engine_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10
