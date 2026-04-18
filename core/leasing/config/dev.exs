import Config

config :mezzanine_leasing, Mezzanine.Leasing.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_leasing_dev",
  stacktrace: true,
  show_sensitive_data_on_connection_error: true,
  pool_size: 10
