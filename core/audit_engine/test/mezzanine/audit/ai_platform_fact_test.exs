defmodule Mezzanine.Audit.AIPlatformFactTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Audit.AIPlatformFact

  test "memory access audit facts carry bounded refs and no raw bodies" do
    assert {:ok, fact} = AIPlatformFact.memory_access_recorded(memory_attrs())

    assert fact.fact_kind == :memory_access_recorded
    assert fact.installation_id == "installation://a"
    assert fact.trace_id == "trace://a"
    assert fact.payload["operation"] == "write"
    refute Map.has_key?(fact.payload, "body")
    refute Map.has_key?(fact.payload, "raw_body")

    assert {:error, {:raw_ai_platform_audit_payload_forbidden, :memory_body}} =
             memory_attrs()
             |> Map.put(:memory_body, "raw memory")
             |> AIPlatformFact.memory_access_recorded()
  end

  test "budget enforced audit facts carry bounded accounting fields" do
    assert {:ok, fact} = AIPlatformFact.budget_enforced(budget_attrs())

    assert fact.fact_kind == :budget_enforced
    assert fact.payload["locus"] == "preflight"
    assert fact.payload["decision_class"] == "deny_exhausted"
    assert fact.payload["requested_units"] == 10

    assert {:error, {:invalid_ai_platform_audit_field, :residual_units}} =
             budget_attrs()
             |> Map.put(:residual_units, -1)
             |> AIPlatformFact.budget_enforced()
  end

  defp memory_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-memory-audit",
      trace_ref: "trace://a",
      scope_key: "memory-scope://a",
      operation: :write,
      redaction_policy_ref: "redaction-policy://memory",
      memory_id: "memory://a",
      evidence_hash: "sha256:memory"
    }
  end

  defp budget_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-budget-audit",
      trace_ref: "trace://a",
      locus: :preflight,
      decision_class: :deny_exhausted,
      requested_units: 10,
      granted_units: 0,
      residual_units: 0,
      policy_revision_ref: "policy-revision://budget"
    }
  end
end
