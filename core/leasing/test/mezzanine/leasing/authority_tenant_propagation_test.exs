defmodule Mezzanine.Leasing.AuthorityTenantPropagationTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Leasing.AuthorityTenantPropagation
  alias Mezzanine.Leasing.AuthorizationScope

  test "declares the Mezzanine owner evidence surface for AuthorityTenantPropagation.v1" do
    contract = AuthorityTenantPropagation.contract()

    assert contract.id == "AuthorityTenantPropagation.v1"
    assert contract.owner == :mezzanine
    assert contract.consumes_owner_contract == "AuthorityTenantPropagation.v1"

    assert contract.required_owner_fields == [
             :tenant_ref,
             :authority_decision_ref,
             :budget_ref,
             :no_bypass_scope_ref,
             :lineage_ref,
             :causation_ref,
             :idempotency_ref,
             :lower_facts_propagation_ref
           ]
  end

  test "populates AuthorizationScope evidence from owner facts" do
    assert {:ok, evidence} =
             AuthorityTenantPropagation.authorization_scope_evidence(
               AuthorityTenantPropagation.fixture()
             )

    assert %AuthorizationScope{} = evidence.authorization_scope
    assert evidence.authorization_scope.tenant_id == "tenant-phase6-m8"
    assert evidence.authorization_scope.execution_id == "exec-phase6-m8"

    assert evidence.authorization_scope_ref ==
             "authorization-scope://tenant-phase6-m8/exec-phase6-m8"

    assert evidence.authority_decision_ref == "authority-decision:phase6-m8"
    assert evidence.budget_ref == "budget://phase6/m8/local-no-spend"
    assert evidence.no_bypass_scope_ref == "no-bypass://phase6/m8/authority-tenant-budget"
    assert evidence.lower_facts_propagation_ref == "lower-facts://tenant-phase6-m8/run-phase6-m8"
    refute evidence.forbidden_present?
  end

  test "fails closed for missing authority, missing budget, and cross tenant scope" do
    fixture = AuthorityTenantPropagation.fixture()

    assert {:error, :missing_authority_decision_ref} =
             fixture
             |> Map.put(:authority_decision_ref, nil)
             |> AuthorityTenantPropagation.authorization_scope_evidence()

    assert {:error, :missing_budget_ref} =
             fixture
             |> Map.put(:budget_ref, "")
             |> AuthorityTenantPropagation.authorization_scope_evidence()

    assert {:error, {:cross_tenant_scope, "tenant-other"}} =
             fixture
             |> put_in([:authorization_scope_attrs, :tenant_id], "tenant-other")
             |> AuthorityTenantPropagation.authorization_scope_evidence()
  end

  test "rejects lower facts mismatch and direct lower bypass" do
    fixture = AuthorityTenantPropagation.fixture()

    assert {:error, {:lower_facts_tenant_mismatch, "tenant-other"}} =
             fixture
             |> put_in([:lower_facts, :tenant_id], "tenant-other")
             |> AuthorityTenantPropagation.authorization_scope_evidence()

    assert {:error, {:forbidden_evidence, :direct_lower_shortcut_bypassing_authority}} =
             fixture
             |> put_in([:lower_facts, :shortcut?], true)
             |> AuthorityTenantPropagation.authorization_scope_evidence()
  end

  test "requires explicit no-bypass scope and lower facts propagation refs" do
    fixture = AuthorityTenantPropagation.fixture()

    assert {:error, :missing_no_bypass_scope_ref} =
             fixture
             |> Map.delete(:no_bypass_scope_ref)
             |> AuthorityTenantPropagation.authorization_scope_evidence()

    assert {:error, {:lower_facts_propagation_ref_mismatch, "lower-facts://other"}} =
             fixture
             |> put_in([:lower_facts, :propagation_ref], "lower-facts://other")
             |> AuthorityTenantPropagation.authorization_scope_evidence()
  end
end
