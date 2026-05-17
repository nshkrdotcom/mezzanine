defmodule MezzaninePackModelTest do
  use ExUnit.Case

  alias Mezzanine.Lifecycle.SubjectSnapshot

  alias Mezzanine.Pack.{
    BindingSpec,
    CompiledPack,
    CompiledOperationGraph,
    CompiledOperationRole,
    ContextSourceSpec,
    EvidenceBinding,
    OperationDependency,
    OperationGraph,
    OperationRole,
    ResourceEffectBinding,
    RuntimeBinding,
    SourceBinding,
    SourcePublicationBinding,
    ToolBinding,
    WorkflowSpec
  }

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

  test "generic binding records are explicit typed pack data" do
    source = %SourceBinding{
      binding_ref: :document_source,
      source_kind: :document,
      subject_kind: :review_document,
      connector_ref: :local_document_http,
      manifest_ref: :local_document_manifest,
      operation_refs: %{read: :document_read},
      credential_binding_ref: :document_http_credential
    }

    publication = %SourcePublicationBinding{
      binding_ref: :document_publication,
      source_binding_ref: :document_source,
      connector_ref: :local_document_http,
      manifest_ref: :local_document_manifest,
      operation_refs: %{publish: :review_publish},
      credential_binding_ref: :document_http_credential,
      template_ref: :review_summary,
      publication_profile_ref: :document_review_publication
    }

    runtime = %RuntimeBinding{
      binding_ref: :deterministic_review_runtime,
      runtime_family: :direct,
      connector_ref: :local_document_http,
      manifest_ref: :local_document_manifest,
      operation_refs: %{run: :review_run},
      credential_binding_ref: :document_http_credential
    }

    tool = %ToolBinding{
      binding_ref: :review_lookup_tool,
      runtime_binding_ref: :deterministic_review_runtime,
      connector_ref: :local_document_http,
      manifest_ref: :local_document_manifest,
      operation_refs: %{lookup: :document_lookup},
      authorization_class: :runtime_tool_invocation,
      credential_binding_ref: :document_http_credential
    }

    evidence = %EvidenceBinding{
      binding_ref: :review_evidence,
      evidence_kind: :review_report,
      connector_ref: :local_document_http,
      manifest_ref: :local_document_manifest,
      operation_refs: %{collect: :review_evidence_collect},
      credential_binding_ref: :document_http_credential
    }

    effect = %ResourceEffectBinding{
      binding_ref: :review_state_update,
      effect_kind: :review_state_update,
      connector_ref: :local_document_http,
      manifest_ref: :local_document_manifest,
      operation_refs: %{update: :review_state_update},
      operation_group_ref: :review_write_effects,
      credential_binding_ref: :document_http_credential,
      confirmation_policy_ref: :operator_confirm_review_write
    }

    assert BindingSpec.kind(source) == :source
    assert BindingSpec.kind(publication) == :source_publication
    assert BindingSpec.kind(runtime) == :runtime
    assert BindingSpec.kind(tool) == :runtime_tool
    assert BindingSpec.kind(evidence) == :evidence
    assert BindingSpec.kind(effect) == :resource_effect
  end

  test "workflow operation graphs are explicit runtime configuration data" do
    role = %OperationRole{
      role_ref: :deterministic_review,
      binding_ref: :deterministic_review_runtime,
      operation_role: :run,
      operation_class: :runtime_operation,
      projection_order_key: 1
    }

    dependency = %OperationDependency{
      from_role: :deterministic_review,
      to_role: :review_publication,
      relation: :blocks_on_success
    }

    graph = %OperationGraph{
      graph_ref: :document_review_graph,
      workflow_ref: :document_review_workflow,
      roles: [role],
      dependencies: [dependency]
    }

    workflow = %WorkflowSpec{
      workflow_ref: :document_review_workflow,
      runtime_role_ref: :deterministic_review,
      operation_graph_ref: :document_review_graph
    }

    compiled_role = %CompiledOperationRole{
      role_ref: "deterministic_review",
      binding_ref: "deterministic_review_runtime",
      binding_kind: :runtime,
      operation_role: "run",
      operation_ref: "review_run",
      operation_class: :runtime_operation,
      projection_order_key: 1
    }

    compiled_graph = %CompiledOperationGraph{
      graph_ref: "document_review_graph",
      workflow_ref: "document_review_workflow",
      roles: [compiled_role],
      roles_by_ref: %{"deterministic_review" => compiled_role},
      dependencies: []
    }

    assert graph.roles == [role]
    assert graph.dependencies == [dependency]
    assert workflow.runtime_role_ref == :deterministic_review
    assert compiled_graph.roles_by_ref["deterministic_review"].operation_ref == "review_run"
  end
end
