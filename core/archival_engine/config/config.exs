import Config

config :ash,
  domains: [
    Mezzanine.Archival,
    Mezzanine.EvidenceLedger,
    Mezzanine.Decisions,
    Mezzanine.Execution,
    Mezzanine.Objects,
    Mezzanine.Audit
  ]

config :mezzanine_archival_engine,
  ecto_repos: [Mezzanine.Archival.Repo],
  ash_domains: [Mezzanine.Archival]

config :mezzanine_evidence_engine,
  ecto_repos: [Mezzanine.EvidenceLedger.Repo],
  ash_domains: [Mezzanine.EvidenceLedger]

config :mezzanine_decision_engine,
  ecto_repos: [Mezzanine.Decisions.Repo],
  ash_domains: [Mezzanine.Decisions]

config :mezzanine_execution_engine,
  ecto_repos: [Mezzanine.Execution.Repo],
  ash_domains: [Mezzanine.Execution]

config :mezzanine_object_engine,
  ecto_repos: [Mezzanine.Objects.Repo],
  ash_domains: [Mezzanine.Objects]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.AuditDomain]

config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

import_config "#{config_env()}.exs"
