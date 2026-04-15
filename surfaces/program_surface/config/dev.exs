import Config

config :mezzanine_ops_domain, Mezzanine.OpsDomain.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "mezzanine_ops_domain_dev",
  show_sensitive_data_on_connection_error: true,
  pool_size: 10
