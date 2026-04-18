import Config

config :mezzanine_leasing, Mezzanine.Leasing.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_leasing_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2
