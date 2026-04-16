import Config

config :mezzanine_config_registry, Mezzanine.ConfigRegistry.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_config_registry_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 10,
  show_sensitive_data_on_connection_error: true
