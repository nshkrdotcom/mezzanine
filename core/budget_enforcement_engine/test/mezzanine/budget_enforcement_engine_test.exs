defmodule Mezzanine.BudgetEnforcementEngineTest do
  use ExUnit.Case, async: true

  alias Mezzanine.BudgetEnforcementEngine

  test "enforces every fail-closed locus" do
    assert {:ok, budget} = budget_ref(hard_cap_amount: 10, soft_cap_amount: 7)

    for locus <- [:preflight, :append, :stream, :runtime_admission, :reconciliation] do
      assert {:ok, ledger} = BudgetEnforcementEngine.new_ledger()

      assert {:ok, _ledger, decision} =
               BudgetEnforcementEngine.enforce(ledger, budget, %{
                 locus: locus,
                 requested_units: 11
               })

      assert decision.decision_class == :deny_hard_exhausted

      assert decision.operator_actions == [
               :accept_override,
               :reject_override,
               :escalate_override,
               :expire_override
             ]
    end
  end

  test "allows, warns at soft cap, and denies at hard cap deterministically" do
    assert {:ok, budget} = budget_ref(hard_cap_amount: 10, soft_cap_amount: 3)
    assert {:ok, ledger} = BudgetEnforcementEngine.new_ledger()

    assert {:ok, ledger, allow} =
             BudgetEnforcementEngine.enforce(ledger, budget, %{
               locus: :preflight,
               requested_units: 4
             })

    assert allow.decision_class == :allow

    assert {:ok, ledger, warn} =
             BudgetEnforcementEngine.enforce(ledger, budget, %{
               locus: :append,
               requested_units: 4
             })

    assert warn.decision_class == :allow_warn_soft

    assert {:ok, _ledger, deny} =
             BudgetEnforcementEngine.enforce(ledger, budget, %{
               locus: :runtime_admission,
               requested_units: 4
             })

    assert deny.decision_class == :deny_hard_exhausted
  end

  test "override requires permission, reason, and bounded duration" do
    assert {:ok, budget} = budget_ref(hard_cap_amount: 1, soft_cap_amount: 1)
    assert {:ok, ledger} = BudgetEnforcementEngine.new_ledger()

    assert {:ok, _ledger, denied} =
             BudgetEnforcementEngine.enforce(ledger, budget, %{
               locus: :preflight,
               requested_units: 2,
               override_permission_ref: "permission://budget/read",
               reason_ref: "reason://operator",
               duration_seconds: 60
             })

    assert denied.decision_class == :deny_hard_exhausted

    assert {:ok, _ledger, allowed} =
             BudgetEnforcementEngine.enforce(ledger, budget, %{
               locus: :preflight,
               requested_units: 2,
               override_permission_ref: "permission://budget/override",
               reason_ref: "reason://operator",
               duration_seconds: 60
             })

    assert allowed.decision_class == :allow_with_override
  end

  test "durable postgres adapter is opt-in and rejected until registered" do
    assert {:error, :budget_postgres_adapter_not_registered} =
             BudgetEnforcementEngine.new_ledger(tier: {:durable, :postgres})
  end

  defp budget_ref(overrides) do
    defaults = %{
      budget_id: "budget://phase-d/default",
      tenant_ref: "tenant://a",
      installation_ref: "installation://a",
      run_ref: "run://a",
      period_class: :per_run,
      hard_cap_amount: 10,
      soft_cap_amount: 5,
      override_policy_ref: "policy://budget/override/default"
    }

    attrs = Enum.into(overrides, defaults)
    BudgetEnforcementEngine.budget_ref(attrs)
  end
end
