import Config

config :mezzanine_core, Mezzanine.Repo,
  database: "nshkr_mezzanine",
  hostname: "localhost",
  password: "postgres",
  pool_size: 10,
  show_sensitive_data_on_connection_error: false,
  username: "postgres"
