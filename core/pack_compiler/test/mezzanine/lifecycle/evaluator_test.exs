defmodule Mezzanine.Lifecycle.EvaluatorTest do
  use ExUnit.Case

  alias Mezzanine.Lifecycle.{Evaluator, SubjectSnapshot}
  alias Mezzanine.Pack.Compiler

  setup do
    {:ok, compiled} = Compiler.compile(Mezzanine.TestPacks.ExpenseApprovalPack)
    %{compiled: compiled}
  end

  test "prefers failure-kind-specific transitions before the generic fallback", %{
    compiled: compiled
  } do
    subject =
      SubjectSnapshot.new(
        subject_kind: :expense_request,
        lifecycle_state: :submitted,
        payload: %{"amount_cents" => 42_00},
        evidence_summary: %{},
        decisions: %{}
      )

    assert {:ok, %{to: "needs_correction"}} =
             Evaluator.can_transition?(
               compiled,
               subject,
               {:execution_failed, :policy_check, :semantic_failure}
             )
  end

  test "falls back to the generic execution failure transition when no specific match exists", %{
    compiled: compiled
  } do
    subject =
      SubjectSnapshot.new(
        subject_kind: :expense_request,
        lifecycle_state: :submitted,
        payload: %{"amount_cents" => 42_00},
        evidence_summary: %{},
        decisions: %{}
      )

    assert {:ok, %{to: "retry_submission"}} =
             Evaluator.can_transition?(
               compiled,
               subject,
               {:execution_failed, :policy_check, :timeout}
             )
  end

  test "guards must pass before a lifecycle transition is allowed", %{compiled: compiled} do
    subject =
      SubjectSnapshot.new(
        subject_kind: :expense_request,
        lifecycle_state: :awaiting_manager_review,
        payload: %{"amount_cents" => 42_00},
        evidence_summary: %{receipt: :pending},
        decisions: %{manager_review: :accept}
      )

    assert {:error, :guard_failed} =
             Evaluator.can_transition?(
               compiled,
               subject,
               {:decision_made, :manager_review, :accept}
             )
  end

  test "guards can unlock an otherwise valid transition", %{compiled: compiled} do
    subject =
      SubjectSnapshot.new(
        subject_kind: :expense_request,
        lifecycle_state: :awaiting_manager_review,
        payload: %{"amount_cents" => 42_00},
        evidence_summary: %{receipt: :collected},
        decisions: %{manager_review: :accept}
      )

    assert {:ok, %{to: "approved"}} =
             Evaluator.can_transition?(
               compiled,
               subject,
               {:decision_made, :manager_review, :accept}
             )
  end
end
