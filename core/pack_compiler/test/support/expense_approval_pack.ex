defmodule Mezzanine.TestPacks.ExpenseApprovalGuards do
  @moduledoc false

  alias Mezzanine.Pack.SubjectContext

  def receipt_collected?(%SubjectContext{evidence_summary: evidence_summary}) do
    Map.get(evidence_summary, "receipt") == :collected
  end
end

defmodule Mezzanine.TestPacks.ExpenseApprovalPack do
  @moduledoc false

  @behaviour Mezzanine.Pack

  alias Mezzanine.Pack.{
    ContextSourceSpec,
    DecisionSpec,
    EvidenceSpec,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    OperatorActionSpec,
    ProjectionSpec,
    SourceKindSpec,
    SubjectKindSpec
  }

  @impl true
  def manifest do
    %Manifest{
      pack_slug: :expense_approval,
      version: "1.0.0",
      description: "Proof pack for the neutral mezzanine compiler",
      subject_kind_specs: [
        %SubjectKindSpec{
          name: :expense_request,
          description: "One expense request subject",
          payload_schema: %{amount_cents: :integer, receipt_required: :boolean}
        }
      ],
      source_kind_specs: [
        %SourceKindSpec{
          name: :expense_form,
          subject_kind: :expense_request,
          description: "Submitted expense form"
        }
      ],
      context_source_specs: [
        %ContextSourceSpec{
          source_ref: :workspace_memory,
          description: "Read-only workspace memory context",
          binding_key: :shared_memory,
          usage_phase: :retrieval,
          required?: false,
          timeout_ms: 750,
          schema_ref: "context/workspace_memory",
          max_fragments: 4,
          merge_strategy: :ranked_append
        }
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :expense_request,
          initial_state: :submitted,
          terminal_states: [:approved, :rejected, :expired],
          transitions: [
            %{
              from: :submitted,
              to: :awaiting_manager_review,
              trigger: {:execution_completed, :policy_check}
            },
            %{
              from: :submitted,
              to: :needs_correction,
              trigger: {:execution_failed, :policy_check, :semantic_failure}
            },
            %{
              from: :submitted,
              to: :retry_submission,
              trigger: {:execution_failed, :policy_check}
            },
            %{
              from: :needs_correction,
              to: :submitted,
              trigger: {:operator_action, :collect_receipt}
            },
            %{
              from: :retry_submission,
              to: :submitted,
              trigger: :auto
            },
            %{
              from: :awaiting_manager_review,
              to: :approved,
              trigger: {:decision_made, :manager_review, :accept},
              guard: %{
                module: Mezzanine.TestPacks.ExpenseApprovalGuards,
                function: :receipt_collected?
              }
            },
            %{
              from: :awaiting_manager_review,
              to: :rejected,
              trigger: {:decision_made, :manager_review, :reject}
            },
            %{
              from: :awaiting_manager_review,
              to: :expired,
              trigger: {:decision_made, :manager_review, :expired}
            }
          ]
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: :policy_check,
          description: "Evaluate the expense request",
          runtime_class: :workflow,
          placement_ref: :policy_worker,
          required_lifecycle_hints: [:receipt_status],
          retry_config: %{
            max_attempts: 2,
            backoff: :linear,
            retry_on: [:transient_failure, :timeout]
          },
          workspace_policy: %{
            strategy: :per_subject,
            reuse: true,
            cleanup: :on_terminal,
            root_ref: :expense_workspaces
          },
          sandbox_policy_ref: :standard_expense_policy,
          prompt_refs: [:expense_policy_prompt],
          execution_params: %{timeout_ms: 60_000},
          applicable_to: [:expense_request]
        }
      ],
      decision_specs: [
        %DecisionSpec{
          decision_kind: :manager_review,
          description: "Manager approval gate",
          trigger: {:after_execution_completed, :policy_check},
          required_evidence_kinds: [:receipt],
          authorized_actors: [:manager],
          allowed_decisions: [:accept, :reject, :expired],
          required_within_hours: 24
        }
      ],
      evidence_specs: [
        %EvidenceSpec{
          evidence_kind: :receipt,
          description: "Uploaded receipt",
          collector_ref: :receipt_store,
          collection_strategy: :manual,
          collected_on: {:subject_entered_state, :submitted}
        }
      ],
      operator_action_specs: [
        %OperatorActionSpec{
          action_kind: :collect_receipt,
          description: "Collect the missing receipt",
          applicable_states: [:submitted, :needs_correction],
          authorized_roles: [:finance],
          effect: {:collect_evidence, :receipt}
        }
      ],
      projection_specs: [
        %ProjectionSpec{
          name: :manager_queue,
          description: "Pending manager review queue",
          subject_kinds: [:expense_request],
          default_filters: %{lifecycle_state: "awaiting_manager_review"},
          sort: [{:inserted_at, :asc}],
          included_fields: [:subject_kind, :lifecycle_state]
        }
      ]
    }
  end
end
