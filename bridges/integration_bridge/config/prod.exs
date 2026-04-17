import Config

config :mezzanine_audit_engine, Mezzanine.Audit.Repo,
  username: System.fetch_env!("PGUSER"),
  password: System.fetch_env!("PGPASSWORD"),
  hostname: System.fetch_env!("PGHOST"),
  port: String.to_integer(System.get_env("PGPORT", "5432")),
  database: System.fetch_env!("MEZZANINE_AUDIT_DB"),
  pool_size: String.to_integer(System.get_env("POOL_SIZE", "10"))

config :mezzanine_ops_domain, Mezzanine.OpsDomain.Repo,
  username: System.fetch_env!("PGUSER"),
  password: System.fetch_env!("PGPASSWORD"),
  hostname: System.fetch_env!("PGHOST"),
  port: String.to_integer(System.get_env("PGPORT", "5432")),
  database: System.fetch_env!("MEZZANINE_OPS_DOMAIN_DB"),
  pool_size: String.to_integer(System.get_env("POOL_SIZE", "10"))
