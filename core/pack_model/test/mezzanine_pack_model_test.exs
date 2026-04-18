defmodule MezzaninePackModelTest do
  use ExUnit.Case

  alias Mezzanine.Lifecycle.SubjectSnapshot
  alias Mezzanine.Pack.{CompiledPack, ContextSourceSpec}
  alias Mezzanine.Pack.SubjectContext

  test "subject snapshots canonicalize pack identifiers to strings" do
    snapshot =
      SubjectSnapshot.new(
        subject_kind: :expense_request,
        lifecycle_state: :awaiting_manager_review,
        payload: %{"amount_cents" => 42_00},
        evidence_summary: %{receipt: :collected},
        decisions: %{manager_review: :accept}
      )

    assert snapshot.subject_kind == "expense_request"
    assert snapshot.lifecycle_state == "awaiting_manager_review"
    assert snapshot.evidence_summary == %{"receipt" => :collected}
    assert snapshot.decisions == %{"manager_review" => :accept}
  end

  test "subject contexts can be built from lifecycle snapshots" do
    snapshot =
      SubjectSnapshot.new(
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 42_00},
        evidence_summary: %{"receipt" => :pending},
        decisions: %{"manager_review" => :expired}
      )

    assert %SubjectContext{
             payload: %{"amount_cents" => 42_00},
             evidence_summary: %{"receipt" => :pending},
             decisions: %{"manager_review" => :expired}
           } = SubjectContext.from_snapshot(snapshot)
  end

  test "compiled packs namespace transition lookup by subject kind and state" do
    compiled = %CompiledPack{
      context_sources_by_ref: %{
        "workspace_memory" => %ContextSourceSpec{
          source_ref: "workspace_memory",
          binding_key: "memory_adapter",
          usage_phase: :retrieval,
          timeout_ms: 750,
          max_fragments: 3
        }
      },
      transitions_by_state: %{
        {"expense_request", "submitted"} => %{
          {:execution_completed, "policy_check"} => %{to: "review"}
        }
      }
    }

    assert CompiledPack.transitions_for(compiled, :expense_request, :submitted) == %{
             {:execution_completed, "policy_check"} => %{to: "review"}
           }

    assert compiled.context_sources_by_ref["workspace_memory"].binding_key == "memory_adapter"
  end
end
