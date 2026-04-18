import Config

config :mezzanine_leasing,
  ecto_repos: [Mezzanine.Leasing.Repo],
  default_read_ttl_ms: 300_000,
  default_stream_ttl_ms: 120_000,
  default_poll_interval_ms: 2_000

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

import_config "#{config_env()}.exs"
