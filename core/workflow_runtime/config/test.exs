import Config

config :logger, level: :warning

config :mezzanine_workflow_runtime, allow_test_outbox_store?: true

config :mezzanine_ops_domain, Mezzanine.OpsDomain.Repo,
  database: "mezzanine_workflow_runtime_test",
  hostname: "localhost",
  password: "postgres",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 4,
  show_sensitive_data_on_connection_error: false,
  username: "postgres"

config :mezzanine_ops_domain, start_runtime_children?: false
