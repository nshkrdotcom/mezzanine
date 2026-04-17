defmodule Mezzanine.Execution.RuntimeStackTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Execution.RuntimeStack

  test "exposes the full runtime repo and domain inventory through neutral helpers" do
    assert RuntimeStack.ops_domain_repo() == Mezzanine.OpsDomain.Repo

    assert RuntimeStack.repo_modules() == [
             Mezzanine.Audit.Repo,
             Mezzanine.ConfigRegistry.Repo,
             Mezzanine.Decisions.Repo,
             Mezzanine.EvidenceLedger.Repo,
             Mezzanine.Execution.Repo,
             Mezzanine.OpsDomain.Repo
           ]

    assert RuntimeStack.ops_domain_ash_domains() == [
             Mezzanine.Programs,
             Mezzanine.Work,
             Mezzanine.Runs,
             Mezzanine.Review,
             Mezzanine.Evidence,
             Mezzanine.Control
           ]

    assert RuntimeStack.ash_domains() == [
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
  end

  test "keeps migration inventory aligned with the repo inventory" do
    assert Enum.map(RuntimeStack.migration_components(), &elem(&1, 0)) ==
             RuntimeStack.repo_modules()
  end
end
