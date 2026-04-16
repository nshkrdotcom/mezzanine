import Config

config :mezzanine_audit_engine, Mezzanine.Audit.Repo,
  username: System.get_env("PGUSER", "postgres"),
  password: System.get_env("PGPASSWORD", "postgres"),
  hostname: System.get_env("PGHOST", "localhost"),
  port: String.to_integer(System.get_env("PGPORT", "5432")),
  database: "mezzanine_audit_engine_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 2
