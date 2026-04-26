defmodule Mezzanine.Pack.CompilerTest do
  use ExUnit.Case

  alias Mezzanine.Pack.{
    CompiledPack,
    ContextSourceSpec,
    DecisionSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    SourceBindingSpec,
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

  test "compiles coding-ops source bindings, publish rules, and runtime policy" do
    assert {:ok, compiled} = Compiler.compile(coding_ops_manifest())

    source_binding = compiled.source_bindings_by_ref["linear_primary"]

    assert source_binding.source_kind == "linear_issue"
    assert source_binding.connection_ref == "linear-prod"
    assert source_binding.state_mapping["candidate"] == ["Todo"]

    publish_rule = compiled.source_publishers_by_ref["linear_workpad"]

    assert publish_rule.source_binding_ref == "linear_primary"
    assert publish_rule.operation == :update_comment
    assert publish_rule.trigger == {:subject_entered_state, "running"}

    recipe = compiled.recipes_by_ref["run_coding_agent"]

    assert recipe.workspace_policy.root_ref == "local_workspaces"
    assert recipe.sandbox_policy_ref == "standard_coding"
    assert recipe.prompt_refs == ["coding_agent_system"]
    assert recipe.dynamic_tool_manifest.tools == ["linear.comment.update", "github.pr.create"]
    assert recipe.hook_stages == [:prepare_workspace, :after_turn]
    assert recipe.max_turns == 12
    assert recipe.stall_timeout_ms == 300_000
  end

  test "rejects invalid state mappings, missing connector bindings, and missing workspace roots" do
    manifest = %Manifest{
      coding_ops_manifest()
      | source_binding_specs: [
          %SourceBindingSpec{
            binding_ref: :linear_primary,
            source_kind: :linear_issue,
            subject_kind: :coding_task,
            provider: :linear,
            state_mapping: %{unknown_state: ["Todo"]}
          }
        ],
        source_publish_specs: [],
        execution_recipe_specs: [
          %ExecutionRecipeSpec{
            recipe_ref: :run_coding_agent,
            runtime_class: :workflow,
            placement_ref: :codex_local,
            workspace_policy: %{strategy: :per_subject},
            sandbox_policy_ref: nil,
            prompt_refs: [],
            applicable_to: [:coding_task]
          }
        ]
    }

    assert {:error, issues} = Compiler.compile(manifest)

    messages = Enum.map(issues, & &1.message)

    assert Enum.any?(messages, &String.contains?(&1, "connection_ref"))
    assert Enum.any?(messages, &String.contains?(&1, "unknown lifecycle state"))
    assert Enum.any?(messages, &String.contains?(&1, "workspace root"))
    assert Enum.any?(messages, &String.contains?(&1, "sandbox_policy_ref"))
    assert Enum.any?(messages, &String.contains?(&1, "prompt_refs"))
  end

  defp coding_ops_manifest do
    %Manifest{
      pack_slug: :coding_ops,
      version: "1.0.0",
      subject_kind_specs: [%SubjectKindSpec{name: :coding_task}],
      source_kind_specs: [
        %Mezzanine.Pack.SourceKindSpec{
          name: :linear_issue,
          subject_kind: :coding_task
        }
      ],
      source_binding_specs: [
        %SourceBindingSpec{
          binding_ref: :linear_primary,
          source_kind: :linear_issue,
          subject_kind: :coding_task,
          provider: :linear,
          connection_ref: :"linear-prod",
          state_mapping: %{
            candidate: ["Todo"],
            submitted: ["In Progress"],
            running: ["In Progress"],
            completed: ["Done"]
          },
          candidate_filters: %{team: "ENG"},
          cursor_policy: %{poll_every_ms: 60_000}
        }
      ],
      source_publish_specs: [
        %Mezzanine.Pack.SourcePublishSpec{
          publish_ref: :linear_workpad,
          source_binding_ref: :linear_primary,
          trigger: {:subject_entered_state, :running},
          operation: :update_comment,
          template_ref: :workpad_running,
          idempotency_scope: :subject
        }
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :coding_task,
          initial_state: :candidate,
          terminal_states: [:completed],
          transitions: [
            %{from: :candidate, to: :submitted, trigger: :auto},
            %{from: :submitted, to: :running, trigger: {:execution_requested, :run_coding_agent}},
            %{from: :running, to: :completed, trigger: {:execution_completed, :run_coding_agent}}
          ]
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: :run_coding_agent,
          runtime_class: :workflow,
          placement_ref: :codex_local,
          workspace_policy: %{
            strategy: :per_subject,
            reuse: true,
            cleanup: :on_terminal,
            root_ref: :local_workspaces
          },
          sandbox_policy_ref: :standard_coding,
          prompt_refs: [:coding_agent_system],
          dynamic_tool_manifest: %{tools: ["linear.comment.update", "github.pr.create"]},
          hook_stages: [:prepare_workspace, :after_turn],
          max_turns: 12,
          stall_timeout_ms: 300_000,
          applicable_to: [:coding_task]
        }
      ]
    }
  end
end
