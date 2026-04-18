defmodule Mezzanine.Pack.CompilerTest do
  use ExUnit.Case

  alias Mezzanine.Pack.{
    CompiledPack,
    ContextSourceSpec,
    DecisionSpec,
    LifecycleSpec,
    Manifest,
    SubjectKindSpec
  }

  alias Mezzanine.Pack.Compiler

  test "compiles a neutral proof pack into canonical runtime indices" do
    assert {:ok, %CompiledPack{} = compiled} =
             Compiler.compile(Mezzanine.TestPacks.ExpenseApprovalPack)

    assert compiled.pack_slug == "expense_approval"
    assert compiled.subject_kinds["expense_request"].name == "expense_request"
    assert compiled.recipes_by_ref["policy_check"].recipe_ref == "policy_check"
    assert compiled.recipes_by_ref["policy_check"].required_lifecycle_hints == ["receipt_status"]
    assert compiled.context_sources_by_ref["workspace_memory"].binding_key == "shared_memory"
    assert compiled.context_sources_by_ref["workspace_memory"].usage_phase == :retrieval

    assert compiled.recipes_by_subject_kind["expense_request"] |> Enum.map(& &1.recipe_ref) == [
             "policy_check"
           ]

    assert compiled.transitions_by_state[{"expense_request", "submitted"}][
             {:execution_failed, "policy_check", :semantic_failure}
           ].to == "needs_correction"

    assert compiled.decision_triggers_by_event[{:execution_completed, "policy_check"}]
           |> Enum.map(& &1.decision_kind) == ["manager_review"]

    assert compiled.evidence_triggers_by_event[{:subject_entered_state, "submitted"}]
           |> Enum.map(& &1.evidence_kind) == ["receipt"]
  end

  test "returns aggregated validation errors for invalid manifests" do
    manifest = %Manifest{
      pack_slug: :invalid_expense_pack,
      version: "0.0.1",
      subject_kind_specs: [
        %SubjectKindSpec{name: :expense_request},
        %SubjectKindSpec{name: :expense_request}
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :expense_request,
          initial_state: :submitted,
          terminal_states: [:approved],
          transitions: [
            %{from: :submitted, to: :review, trigger: {:execution_completed, :missing_recipe}}
          ]
        }
      ],
      decision_specs: [
        %DecisionSpec{
          decision_kind: :manager_review,
          trigger: {:after_execution_completed, :missing_recipe},
          required_evidence_kinds: [:missing_receipt],
          required_within_hours: 24
        }
      ]
    }

    assert {:error, issues} = Compiler.compile(manifest)

    messages = Enum.map(issues, & &1.message)

    assert Enum.any?(messages, &String.contains?(&1, "must be unique"))
    assert Enum.any?(messages, &String.contains?(&1, "missing_recipe"))
    assert Enum.any?(messages, &String.contains?(&1, "missing_receipt"))

    assert Enum.any?(
             messages,
             &String.contains?(
               &1,
               "requires a {:decision_made, kind, :expired} lifecycle transition"
             )
           )
  end

  test "returns validation errors for duplicate lifecycle hint declarations" do
    manifest = %Manifest{
      pack_slug: :hinty_pack,
      version: "0.0.1",
      subject_kind_specs: [%SubjectKindSpec{name: :expense_request}],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :expense_request,
          initial_state: :submitted,
          terminal_states: [:paid],
          transitions: [
            %{from: :submitted, to: :processing, trigger: {:execution_requested, :policy_check}},
            %{from: :processing, to: :paid, trigger: {:execution_completed, :policy_check}}
          ]
        }
      ],
      execution_recipe_specs: [
        %Mezzanine.Pack.ExecutionRecipeSpec{
          recipe_ref: :policy_check,
          runtime_class: :session,
          placement_ref: :policy_worker,
          required_lifecycle_hints: [:receipt_status, "receipt_status"]
        }
      ]
    }

    assert {:error, issues} = Compiler.compile(manifest)

    assert Enum.any?(issues, fn issue ->
             String.contains?(issue.message, "required lifecycle hint") and
               String.contains?(issue.message, "must be unique")
           end)
  end

  test "returns validation errors for invalid context source declarations" do
    manifest = %Manifest{
      pack_slug: :memory_pack,
      version: "0.0.1",
      subject_kind_specs: [%SubjectKindSpec{name: :expense_request}],
      context_source_specs: [
        %ContextSourceSpec{
          source_ref: :workspace_memory,
          binding_key: :shared_memory,
          usage_phase: :retrieval,
          timeout_ms: 500,
          max_fragments: 3
        },
        %ContextSourceSpec{
          source_ref: :workspace_memory,
          binding_key: :shared_memory,
          usage_phase: :during_turn,
          required?: :yes,
          timeout_ms: 0,
          max_fragments: 0,
          merge_strategy: :shuffle
        }
      ]
    }

    assert {:error, issues} = Compiler.compile(manifest)

    messages = Enum.map(issues, & &1.message)

    assert Enum.any?(messages, &String.contains?(&1, "context source"))
    assert Enum.any?(messages, &String.contains?(&1, "usage_phase"))
    assert Enum.any?(messages, &String.contains?(&1, "required?"))
    assert Enum.any?(messages, &String.contains?(&1, "timeout_ms"))
    assert Enum.any?(messages, &String.contains?(&1, "merge_strategy"))
  end
end
