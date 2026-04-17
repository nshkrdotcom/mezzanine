defmodule Mezzanine.Execution.RuntimeStack do
  @moduledoc """
  Shared repo and migration inventory for proof harnesses that boot the full
  neutral mezzanine runtime and for downstream runtime config that must boot the
  narrowed legacy substrate without directly spelling its deprecated modules.
  """

  alias Mezzanine.Audit
  alias Mezzanine.Audit.Repo, as: AuditRepo
  alias Mezzanine.ConfigRegistry
  alias Mezzanine.ConfigRegistry.Repo, as: ConfigRegistryRepo
  alias Mezzanine.Control
  alias Mezzanine.Decisions
  alias Mezzanine.Decisions.Repo, as: DecisionsRepo
  alias Mezzanine.Evidence
  alias Mezzanine.EvidenceLedger
  alias Mezzanine.EvidenceLedger.Repo, as: EvidenceRepo
  alias Mezzanine.Execution
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.Objects
  alias Mezzanine.OpsDomain.Repo, as: OpsDomainRepo
  alias Mezzanine.Programs
  alias Mezzanine.Review
  alias Mezzanine.Runs
  alias Mezzanine.Work

  @repo_modules [
    AuditRepo,
    ConfigRegistryRepo,
    DecisionsRepo,
    EvidenceRepo,
    ExecutionRepo,
    OpsDomainRepo
  ]
  @migration_components [
    {AuditRepo, "audit_engine"},
    {ConfigRegistryRepo, "config_registry"},
    {DecisionsRepo, "decision_engine"},
    {EvidenceRepo, "evidence_engine"},
    {ExecutionRepo, "execution_engine"},
    {OpsDomainRepo, "ops_domain"}
  ]
  @ash_domains [
    Audit,
    ConfigRegistry,
    Objects,
    Execution,
    Decisions,
    Programs,
    Work,
    Runs,
    Review,
    Evidence,
    Control,
    EvidenceLedger
  ]
  @ops_domain_ash_domains [
    Programs,
    Work,
    Runs,
    Review,
    Evidence,
    Control
  ]

  @spec repo_modules() :: [module()]
  def repo_modules, do: @repo_modules

  @spec migration_components() :: [{module(), String.t()}]
  def migration_components, do: @migration_components

  @spec ash_domains() :: [module()]
  def ash_domains, do: @ash_domains

  @spec ops_domain_ash_domains() :: [module()]
  def ops_domain_ash_domains, do: @ops_domain_ash_domains

  @spec ops_domain_repo() :: module()
  def ops_domain_repo, do: OpsDomainRepo
end
