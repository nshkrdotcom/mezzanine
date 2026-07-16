import Config

config :mezzanine_core, Mezzanine.Repo,
  database: "mezzanine_core_test",
  hostname: "localhost",
  password: "postgres",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 4,
  show_sensitive_data_on_connection_error: false,
  username: "postgres"
