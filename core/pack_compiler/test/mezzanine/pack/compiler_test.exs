defmodule Mezzanine.Pack.CompilerTest do
  use ExUnit.Case

  alias Mezzanine.Pack.{
    CompiledPack,
    ContextSourceSpec,
    DecisionSpec,
    ExecutionRecipeSpec,
    EvidenceBinding,
    LifecycleSpec,
    Manifest,
    OperationDependency,
    OperationGraph,
    OperationRole,
    ResourceEffectBinding,
    RuntimeBinding,
    SourceBinding,
    SourceBindingSpec,
    SourcePublicationBinding,
    SubjectKindSpec,
    ToolBinding,
    WorkflowSpec
  }

  alias Mezzanine.Pack.Compiler

  test "compiles a neutral proof pack into canonical runtime indices" do
    assert {:ok, %CompiledPack{} = compiled} =
             Compiler.compile(Mezzanine.TestPacks.ExpenseApprovalPack)

    assert compiled.pack_slug == "expense_approval"
    assert compiled.subject_kinds["expense_request"].name == "expense_request"
    assert compiled.recipes_by_ref["policy_check"].recipe_ref == "policy_check"
    assert compiled.recipes_by_ref["policy_check"].required_lifecycle_hints == ["receipt_status"]

    assert compiled.recipes_by_ref["policy_check"].dispatch_ref_requirements == %{
             "authority_decision_ref" => "required",
             "connector_binding_ref" => "required",
             "credential_posture_ref" => "credential_lease_or_no_credentials"
           }

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

  test "rejects manifests missing explicit S0 profile slots" do
    manifest = %Manifest{pack_slug: :slotless_pack, version: "0.0.1"}

    assert {:error, issues} = Compiler.compile(manifest)
    assert Enum.any?(issues, &String.contains?(&1.message, "profile_slots"))
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
    assert recipe.dynamic_tool_manifest.tools == ["linear.comments.update", "github.pr.create"]
    assert recipe.hook_stages == [:prepare_workspace, :after_turn]
    assert recipe.max_turns == 12
    assert recipe.stall_timeout_ms == 300_000
  end

  test "compiles coding-ops review evidence policy and operator actions" do
    manifest = %Manifest{
      coding_ops_manifest()
      | lifecycle_specs: [
          %LifecycleSpec{
            subject_kind: :coding_task,
            initial_state: :candidate,
            terminal_states: [:completed, :cancelled],
            transitions: [
              %{from: :candidate, to: :submitted, trigger: :auto},
              %{
                from: :submitted,
                to: :running,
                trigger: {:execution_requested, :run_coding_agent}
              },
              %{
                from: :running,
                to: :awaiting_review,
                trigger: {:execution_completed, :run_coding_agent}
              },
              %{
                from: :awaiting_review,
                to: :completed,
                trigger: {:decision_made, :operator_review, :accept}
              },
              %{
                from: :awaiting_review,
                to: :running,
                trigger: {:decision_made, :operator_review, :reject}
              },
              %{
                from: :awaiting_review,
                to: :awaiting_review,
                trigger: {:decision_made, :operator_review, :escalate}
              },
              %{
                from: :awaiting_review,
                to: :cancelled,
                trigger: {:decision_made, :operator_review, :expired}
              },
              %{from: :running, to: :cancelled, trigger: {:operator_action, :cancel_execution}}
            ]
          }
        ],
        decision_specs: [
          %Mezzanine.Pack.DecisionSpec{
            decision_kind: :operator_review,
            trigger: {:after_execution_completed, :run_coding_agent},
            required_evidence_kinds: [:github_pr, :codex_session],
            authorized_actors: [:operator],
            allowed_decisions: [:accept, :reject, :waive, :expired, :escalate],
            required_within_hours: 72
          }
        ],
        evidence_specs: [
          %Mezzanine.Pack.EvidenceSpec{
            evidence_kind: :github_pr,
            collector_ref: :github_pr_ref,
            collection_strategy: :automatic,
            collected_on: {:execution_completed, :run_coding_agent},
            schema: %{url: :string, number: :integer}
          },
          %Mezzanine.Pack.EvidenceSpec{
            evidence_kind: :codex_session,
            collector_ref: :codex_session_ref,
            collection_strategy: :automatic,
            collected_on: {:execution_completed, :run_coding_agent}
          }
        ],
        operator_action_specs: [
          %Mezzanine.Pack.OperatorActionSpec{
            action_kind: :pause_execution,
            applicable_states: [:running],
            authorized_roles: [:operator],
            effect: :pause_execution
          },
          %Mezzanine.Pack.OperatorActionSpec{
            action_kind: :resume_execution,
            applicable_states: [:running],
            authorized_roles: [:operator],
            effect: :resume_execution
          },
          %Mezzanine.Pack.OperatorActionSpec{
            action_kind: :retry_execution,
            applicable_states: [:cancelled],
            authorized_roles: [:operator],
            effect: :retry_execution
          },
          %Mezzanine.Pack.OperatorActionSpec{
            action_kind: :cancel_execution,
            applicable_states: [:running],
            authorized_roles: [:operator],
            effect: :cancel_active_execution
          }
        ]
    }

    assert {:ok, compiled} = Compiler.compile(manifest)

    review = compiled.decision_specs_by_kind["operator_review"]
    assert review.required_evidence_kinds == ["codex_session", "github_pr"]
    assert review.allowed_decisions == [:accept, :escalate, :expired, :reject, :waive]

    assert compiled.decision_triggers_by_event[{:execution_completed, "run_coding_agent"}] == [
             review
           ]

    assert compiled.evidence_specs_by_kind["github_pr"].schema == %{
             url: :string,
             number: :integer
           }

    assert compiled.evidence_triggers_by_event[{:execution_completed, "run_coding_agent"}]
           |> Enum.map(& &1.evidence_kind) == ["github_pr", "codex_session"]

    assert compiled.operator_actions_by_kind["pause_execution"].effect == :pause_execution
    assert compiled.operator_actions_by_kind["resume_execution"].effect == :resume_execution
    assert compiled.operator_actions_by_kind["retry_execution"].effect == :retry_execution
  end

  test "compiles generic binding records into canonical runtime indices" do
    manifest = generic_binding_manifest()

    assert {:ok, compiled} = Compiler.compile(manifest)

    assert compiled.bindings_by_ref |> Map.keys() |> Enum.sort() == [
             "deterministic_review_runtime",
             "document_publication",
             "document_source",
             "review_evidence",
             "review_lookup_tool",
             "review_state_update"
           ]

    assert compiled.bindings_by_kind |> Map.keys() |> Enum.sort() == [
             :evidence,
             :resource_effect,
             :runtime,
             :runtime_tool,
             :source,
             :source_publication
           ]

    source = compiled.bindings_by_ref["document_source"]
    assert source.operation_refs == %{"read" => "document_read"}
    assert source.connector_ref == "local_document_http"
    assert source.manifest_ref == "local_document_manifest"
    assert source.credential_binding_ref == "document_http_credential"

    assert compiled.bindings_by_kind.source |> Enum.map(& &1.binding_ref) == [
             "document_source"
           ]

    assert compiled.bindings_by_kind.resource_effect
           |> Enum.map(& &1.confirmation_policy_ref) == [
             "operator_confirm_review_write"
           ]

    assert compiled.bindings_by_ref["document_publication"].publication_profile_ref ==
             "document_review_publication"
  end

  test "compiles workflow operation graphs from product role refs to binding operations" do
    assert {:ok, compiled} = Compiler.compile(generic_binding_manifest_with_workflow_graph())

    assert compiled.workflows_by_ref |> Map.keys() == ["document_review_workflow"]
    assert compiled.operation_graphs_by_ref |> Map.keys() == ["document_review_graph"]

    graph = compiled.compiled_operation_graphs_by_ref["document_review_graph"]

    assert graph.workflow_ref == "document_review_workflow"

    assert graph.roles |> Enum.map(& &1.role_ref) == [
             "document_intake",
             "deterministic_review",
             "review_evidence",
             "review_publication",
             "review_state_effect"
           ]

    assert graph.roles_by_ref["document_intake"].binding_ref == "document_source"
    assert graph.roles_by_ref["document_intake"].binding_kind == :source
    assert graph.roles_by_ref["document_intake"].operation_role == "read"
    assert graph.roles_by_ref["document_intake"].operation_ref == "document_read"
    assert graph.roles_by_ref["deterministic_review"].binding_kind == :runtime
    assert graph.roles_by_ref["deterministic_review"].operation_ref == "review_run"
    assert graph.roles_by_ref["review_state_effect"].binding_kind == :resource_effect
    assert graph.roles_by_ref["review_state_effect"].operation_ref == "review_state_update"

    assert graph.dependencies |> Enum.map(&{&1.from_role, &1.to_role, &1.relation}) == [
             {"document_intake", "deterministic_review", :blocks_on_success},
             {"deterministic_review", "review_evidence", :parallel_allowed},
             {"deterministic_review", "review_publication", :blocks_on_success},
             {"review_evidence", "review_publication", :blocks_on_review},
             {"review_publication", "review_state_effect", :blocks_on_confirmation}
           ]
  end

  test "infers conservative default operation graph dependencies when no edges are authored" do
    manifest = generic_binding_manifest_with_workflow_graph()

    [graph] = manifest.operation_graph_specs
    manifest = %{manifest | operation_graph_specs: [%{graph | dependencies: []}]}

    assert {:ok, compiled} = Compiler.compile(manifest)

    graph = compiled.compiled_operation_graphs_by_ref["document_review_graph"]

    inferred =
      Map.new(graph.dependencies, fn dependency ->
        {{dependency.from_role, dependency.to_role}, dependency}
      end)

    assert inferred[{"document_intake", "review_evidence"}].relation == :parallel_allowed

    assert inferred[{"document_intake", "deterministic_review"}].relation ==
             :blocks_on_success

    assert inferred[{"deterministic_review", "review_publication"}].relation ==
             :blocks_on_success

    assert inferred[{"review_publication", "review_state_effect"}].relation ==
             :blocks_on_success

    assert inferred[{"review_evidence", "review_publication"}].completion_policy == :optional
    assert inferred[{"review_evidence", "review_publication"}].failure_policy == :degrade

    assert Enum.all?(graph.dependencies, &(&1.metadata["inferred"] == true))
    assert length(graph.dependencies) == 10
  end

  test "rejects workflow graphs that bypass binding operation roles" do
    manifest = generic_binding_manifest_with_workflow_graph()

    [graph] = manifest.operation_graph_specs
    [role | rest] = graph.roles

    broken_graph = %{graph | roles: [%{role | operation_role: :missing_read} | rest]}
    manifest = %{manifest | operation_graph_specs: [broken_graph]}

    assert {:error, issues} = Compiler.compile(manifest)

    assert Enum.any?(issues, fn issue ->
             String.contains?(issue.message, "binding operation role") and
               String.contains?(issue.message, "missing_read")
           end)
  end

  test "validates generic binding operations through a credential-free manifest resolver" do
    resolver = fn request ->
      send(self(), {:manifest_lookup_request, request})
      {:ok, descriptor_for(request)}
    end

    assert {:ok, compiled} =
             Compiler.compile(generic_binding_manifest(), manifest_resolver: resolver)

    assert map_size(compiled.bindings_by_ref) == 6

    requests =
      for _index <- 1..6 do
        assert_receive {:manifest_lookup_request, request}
        request
      end

    assert requests |> Enum.map(& &1.operation_ref) |> Enum.sort() == [
             "document_lookup",
             "document_read",
             "review_evidence_collect",
             "review_publish",
             "review_run",
             "review_state_update"
           ]

    assert Map.new(requests, &{&1.operation_ref, &1.operation_role}) == %{
             "document_lookup" => :runtime_tool,
             "document_read" => :source_read,
             "review_evidence_collect" => :evidence_collection,
             "review_publish" => :source_publish,
             "review_run" => :runtime_session,
             "review_state_update" => :resource_effect
           }

    assert Enum.all?(requests, &Map.has_key?(&1.metadata, :pack_operation_role))
    refute Enum.any?(requests, &Map.has_key?(&1, :api_key))
    refute Enum.any?(requests, &Map.has_key?(&1, :access_token))
    refute Enum.any?(requests, &Map.has_key?(&1, :oauth_session))
    refute Enum.any?(requests, &Map.has_key?(&1, :provider_client))
    assert Enum.all?(requests, &Map.has_key?(&1, :credential_scope_ref))
  end

  test "rejects manifest operation descriptors that do not match binding facts" do
    resolver = fn request ->
      descriptor =
        if request.operation_ref == "document_read" do
          request
          |> descriptor_for()
          |> Map.put(:operation_class, :source_write)
        else
          descriptor_for(request)
        end

      {:ok, descriptor}
    end

    assert {:error, issues} =
             Compiler.compile(generic_binding_manifest(), manifest_resolver: resolver)

    messages = Enum.map(issues, & &1.message)

    assert Enum.any?(messages, &String.contains?(&1, "operation_class"))
    assert Enum.any?(messages, &String.contains?(&1, "source_read"))
    assert Enum.any?(messages, &String.contains?(&1, "source_write"))
  end

  test "rejects manifest digest drift and required scope expansion during compilation" do
    manifest = generic_binding_manifest_with_source_manifest_metadata()

    resolver = fn request ->
      descriptor =
        if request.operation_ref == "document_read" do
          request
          |> descriptor_for()
          |> Map.put(:manifest_digest, "sha256:document-manifest-v2")
          |> Map.put(:required_scopes, ["documents.read", "documents.write"])
        else
          descriptor_for(request)
        end

      {:ok, descriptor}
    end

    assert {:error, issues} =
             Compiler.compile(manifest, manifest_resolver: resolver)

    messages = Enum.map(issues, & &1.message)

    assert Enum.any?(messages, &String.contains?(&1, "manifest_digest"))
    assert Enum.any?(messages, &String.contains?(&1, "required scopes expanded"))
    assert Enum.any?(messages, &String.contains?(&1, "documents.write"))
  end

  test "surfaces manifest resolver failures as compiler diagnostics" do
    resolver = fn
      %{operation_ref: "document_lookup"} -> {:error, :operation_missing}
      request -> {:ok, descriptor_for(request)}
    end

    assert {:error, issues} =
             Compiler.compile(generic_binding_manifest(), manifest_resolver: resolver)

    messages = Enum.map(issues, & &1.message)

    assert Enum.any?(messages, &String.contains?(&1, "operation_missing"))
  end

  test "rejects generic binding records that hide operation roles or omit required safety refs" do
    manifest = %Manifest{
      generic_binding_manifest()
      | binding_specs: [
          %SourceBinding{
            binding_ref: :document_source,
            source_kind: :document,
            subject_kind: :review_document,
            connector_ref: :local_document_http,
            manifest_ref: :local_document_manifest,
            operation_refs: [:document_read],
            credential_binding_ref: :document_http_credential
          },
          %ResourceEffectBinding{
            binding_ref: :review_state_update,
            effect_kind: :review_state_update,
            connector_ref: :local_document_http,
            manifest_ref: :local_document_manifest,
            operation_refs: %{update: :review_state_update},
            operation_group_ref: :review_write_effects,
            credential_binding_ref: :document_http_credential
          }
        ]
    }

    assert {:error, issues} = Compiler.compile(manifest)
    messages = Enum.map(issues, & &1.message)

    assert Enum.any?(messages, &String.contains?(&1, "operation_refs"))
    assert Enum.any?(messages, &String.contains?(&1, "confirmation_policy_ref"))
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
      profile_slots: %{
        source_profile_ref: :linear_coding_task,
        runtime_profile_ref: :codex_session,
        tool_scope_ref: :coding_ops_v1,
        evidence_profile_ref: :github_pr_plus_workpad,
        publication_profile_ref: :linear_workpad_review,
        review_profile_ref: :human_operator,
        memory_profile_ref: :none,
        projection_profile_ref: :runtime_readback_v1
      },
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
          dynamic_tool_manifest: %{tools: ["linear.comments.update", "github.pr.create"]},
          hook_stages: [:prepare_workspace, :after_turn],
          max_turns: 12,
          stall_timeout_ms: 300_000,
          applicable_to: [:coding_task]
        }
      ]
    }
  end

  defp generic_binding_manifest do
    %Manifest{
      pack_slug: :toy_document_review,
      version: "1.0.0",
      profile_slots: %{
        source_profile_ref: :document_source_profile,
        runtime_profile_ref: :deterministic_review_runtime,
        tool_scope_ref: :document_review_tools,
        evidence_profile_ref: :review_report_evidence,
        publication_profile_ref: :document_review_publication,
        review_profile_ref: :human_operator,
        memory_profile_ref: :none,
        projection_profile_ref: :document_review_projection
      },
      subject_kind_specs: [%SubjectKindSpec{name: :review_document}],
      binding_specs: [
        %SourceBinding{
          binding_ref: :document_source,
          source_kind: :document,
          subject_kind: :review_document,
          connector_ref: :local_document_http,
          manifest_ref: :local_document_manifest,
          operation_refs: %{read: :document_read},
          credential_binding_ref: :document_http_credential
        },
        %SourcePublicationBinding{
          binding_ref: :document_publication,
          source_binding_ref: :document_source,
          connector_ref: :local_document_http,
          manifest_ref: :local_document_manifest,
          operation_refs: %{publish: :review_publish},
          credential_binding_ref: :document_http_credential,
          template_ref: :review_summary,
          publication_profile_ref: :document_review_publication,
          idempotency_scope: :subject
        },
        %RuntimeBinding{
          binding_ref: :deterministic_review_runtime,
          runtime_family: :direct,
          connector_ref: :local_document_http,
          manifest_ref: :local_document_manifest,
          operation_refs: %{run: :review_run},
          credential_binding_ref: :document_http_credential
        },
        %ToolBinding{
          binding_ref: :review_lookup_tool,
          runtime_binding_ref: :deterministic_review_runtime,
          connector_ref: :local_document_http,
          manifest_ref: :local_document_manifest,
          operation_refs: %{lookup: :document_lookup},
          authorization_class: :runtime_tool_invocation,
          credential_binding_ref: :document_http_credential
        },
        %EvidenceBinding{
          binding_ref: :review_evidence,
          evidence_kind: :review_report,
          connector_ref: :local_document_http,
          manifest_ref: :local_document_manifest,
          operation_refs: %{collect: :review_evidence_collect},
          credential_binding_ref: :document_http_credential
        },
        %ResourceEffectBinding{
          binding_ref: :review_state_update,
          effect_kind: :review_state_update,
          connector_ref: :local_document_http,
          manifest_ref: :local_document_manifest,
          operation_refs: %{update: :review_state_update},
          operation_group_ref: :review_write_effects,
          credential_binding_ref: :document_http_credential,
          confirmation_policy_ref: :operator_confirm_review_write
        }
      ]
    }
  end

  defp generic_binding_manifest_with_workflow_graph do
    %Manifest{
      generic_binding_manifest()
      | operation_graph_specs: [
          %OperationGraph{
            graph_ref: :document_review_graph,
            workflow_ref: :document_review_workflow,
            roles: [
              %OperationRole{
                role_ref: :document_intake,
                binding_ref: :document_source,
                operation_role: :read,
                operation_class: :source_read,
                projection_order_key: 1
              },
              %OperationRole{
                role_ref: :deterministic_review,
                binding_ref: :deterministic_review_runtime,
                operation_role: :run,
                operation_class: :runtime_operation,
                projection_order_key: 2
              },
              %OperationRole{
                role_ref: :review_evidence,
                binding_ref: :review_evidence,
                operation_role: :collect,
                operation_class: :evidence_collection,
                projection_order_key: 3,
                completion_policy: :optional,
                failure_policy: :degrade
              },
              %OperationRole{
                role_ref: :review_publication,
                binding_ref: :document_publication,
                operation_role: :publish,
                operation_class: :source_write,
                projection_order_key: 4
              },
              %OperationRole{
                role_ref: :review_state_effect,
                binding_ref: :review_state_update,
                operation_role: :update,
                operation_class: :resource_effect,
                projection_order_key: 5
              }
            ],
            dependencies: [
              %OperationDependency{
                from_role: :document_intake,
                to_role: :deterministic_review,
                relation: :blocks_on_success
              },
              %OperationDependency{
                from_role: :deterministic_review,
                to_role: :review_evidence,
                relation: :parallel_allowed,
                completion_policy: :optional,
                failure_policy: :degrade
              },
              %OperationDependency{
                from_role: :deterministic_review,
                to_role: :review_publication,
                relation: :blocks_on_success
              },
              %OperationDependency{
                from_role: :review_evidence,
                to_role: :review_publication,
                relation: :blocks_on_review,
                completion_policy: :optional,
                review_policy_ref: :document_review_gate
              },
              %OperationDependency{
                from_role: :review_publication,
                to_role: :review_state_effect,
                relation: :blocks_on_confirmation,
                confirmation_policy_ref: :operator_confirm_review_write
              }
            ]
          }
        ],
        workflow_specs: [
          %WorkflowSpec{
            workflow_ref: :document_review_workflow,
            source_role_ref: :document_intake,
            runtime_role_ref: :deterministic_review,
            publication_role_ref: :review_publication,
            evidence_role_refs: [:review_evidence],
            resource_effect_role_refs: [:review_state_effect],
            operation_graph_ref: :document_review_graph
          }
        ]
    }
  end

  defp generic_binding_manifest_with_source_manifest_metadata do
    manifest = generic_binding_manifest()
    [source | rest] = manifest.binding_specs

    source = %{
      source
      | metadata: %{
          manifest_digest: "sha256:document-manifest-v1",
          required_scopes: %{read: ["documents.read"]}
        }
    }

    %{manifest | binding_specs: [source | rest]}
  end

  defp descriptor_for(request) do
    %{
      connector_ref: request.connector_ref,
      manifest_ref: request.manifest_ref,
      operation_ref: request.operation_ref,
      operation_role: request.operation_role,
      operation_class: request.operation_class,
      binding_kind: request.binding_kind,
      side_effect_class: descriptor_side_effect_class(request.binding_kind),
      input_schema_ref: "schema://#{request.operation_ref}/input",
      output_schema_ref: "schema://#{request.operation_ref}/output",
      credential_scope_ref: request.credential_scope_ref,
      runtime_family: request.required_runtime_family,
      manifest_digest: request.compiled_manifest_hash || "sha256:document-manifest-v1",
      required_scopes: []
    }
  end

  defp descriptor_side_effect_class(:evidence), do: :read
  defp descriptor_side_effect_class(:resource_effect), do: :resource_effect
  defp descriptor_side_effect_class(:runtime), do: :write
  defp descriptor_side_effect_class(:runtime_tool), do: :read
  defp descriptor_side_effect_class(:source), do: :read
  defp descriptor_side_effect_class(:source_publication), do: :write
end
