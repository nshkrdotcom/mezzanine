defmodule Mezzanine.ContextBudgetAdmissionTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ContextBudgetAdmission
  alias OuterBrain.ContextBudget

  test "all loci allow in-budget requests" do
    for locus <- ContextBudgetAdmission.loci() do
      assert {:ok, bucket} = ContextBudget.token_bucket("budget://#{locus}", 10)

      assert {:ok, _updated_bucket, receipt} =
               apply(ContextBudgetAdmission, locus, [bucket, 1, refs()])

      assert receipt.locus == locus
      assert receipt.decision.decision == :allow
    end
  end

  test "all loci fail closed when exhausted" do
    for locus <- ContextBudgetAdmission.loci() do
      assert {:ok, bucket} = ContextBudget.token_bucket("budget://#{locus}", 1)

      assert {:error, {:budget_denied, ^locus, decision}} =
               apply(ContextBudgetAdmission, locus, [bucket, 2, refs()])

      assert decision.decision == :deny_exhausted
    end
  end

  test "required refs are enforced before budget effects" do
    assert {:ok, bucket} = ContextBudget.token_bucket("budget://preflight", 1)

    assert {:error, {:missing_budget_admission_ref, :authority_ref}} =
             ContextBudgetAdmission.preflight(bucket, 1, Map.delete(refs(), :authority_ref))
  end

  defp refs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-budget",
      trace_ref: "trace://a"
    }
  end
end
