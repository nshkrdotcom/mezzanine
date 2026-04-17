import Config

config :mezzanine_runtime_scheduler, Mezzanine.RuntimeScheduler.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_runtime_scheduler_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_execution_engine, Mezzanine.Execution.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_runtime_scheduler_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_object_engine, Mezzanine.Objects.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_runtime_scheduler_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_audit_engine, Mezzanine.Audit.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_runtime_scheduler_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10

config :mezzanine_ops_domain, Mezzanine.OpsDomain.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_runtime_scheduler_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10
