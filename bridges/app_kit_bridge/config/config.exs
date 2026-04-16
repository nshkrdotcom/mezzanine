import Config

config :ash,
  domains: [
    Mezzanine.Audit,
    Mezzanine.ConfigRegistry,
    Mezzanine.Objects,
    Mezzanine.Execution,
    Mezzanine.Decisions,
    Mezzanine.Programs,
    Mezzanine.Work,
    Mezzanine.Runs,
    Mezzanine.Review,
    Mezzanine.Evidence,
    Mezzanine.Control,
    Mezzanine.EvidenceLedger
  ]

config :mezzanine_audit_engine,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.Audit]

config :mezzanine_object_engine,
  ecto_repos: [Mezzanine.Objects.Repo],
  ash_domains: [Mezzanine.Objects]

config :mezzanine_execution_engine,
  ecto_repos: [Mezzanine.Execution.Repo],
  ash_domains: [Mezzanine.Execution]

config :mezzanine_decision_engine,
  ecto_repos: [Mezzanine.Decisions.Repo],
  ash_domains: [Mezzanine.Decisions]

config :mezzanine_evidence_engine,
  ecto_repos: [Mezzanine.EvidenceLedger.Repo],
  ash_domains: [Mezzanine.EvidenceLedger]

config :mezzanine_config_registry,
  ecto_repos: [Mezzanine.ConfigRegistry.Repo],
  ash_domains: [Mezzanine.ConfigRegistry]

config :mezzanine_ops_domain,
  ecto_repos: [Mezzanine.OpsDomain.Repo],
  ash_domains: [
    Mezzanine.Programs,
    Mezzanine.Work,
    Mezzanine.Runs,
    Mezzanine.Review,
    Mezzanine.Evidence,
    Mezzanine.Control
  ]

config :mezzanine_ops_audit,
  ecto_repos: [Mezzanine.Audit.Repo],
  ash_domains: [Mezzanine.Audit]

config :mezzanine_ops_control,
  ecto_repos: [Mezzanine.OpsDomain.Repo],
  ash_domains: [Mezzanine.Control]

config :mezzanine_app_kit_bridge,
  ecto_repos: [
    Mezzanine.Audit.Repo,
    Mezzanine.ConfigRegistry.Repo,
    Mezzanine.Objects.Repo,
    Mezzanine.Execution.Repo,
    Mezzanine.Decisions.Repo,
    Mezzanine.EvidenceLedger.Repo,
    Mezzanine.OpsDomain.Repo
  ]

import_config "#{config_env()}.exs"
