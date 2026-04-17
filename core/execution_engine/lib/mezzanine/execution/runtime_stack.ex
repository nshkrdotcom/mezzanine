defmodule Mezzanine.Execution.RuntimeStack do
  @moduledoc """
  Shared repo and migration inventory for proof harnesses that boot the full
  neutral mezzanine runtime.
  """

  alias Mezzanine.Audit.Repo, as: AuditRepo
  alias Mezzanine.ConfigRegistry.Repo, as: ConfigRegistryRepo
  alias Mezzanine.Decisions.Repo, as: DecisionsRepo
  alias Mezzanine.EvidenceLedger.Repo, as: EvidenceRepo
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.OpsDomain.Repo, as: OpsDomainRepo

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

  @spec repo_modules() :: [module()]
  def repo_modules, do: @repo_modules

  @spec migration_components() :: [{module(), String.t()}]
  def migration_components, do: @migration_components
end
