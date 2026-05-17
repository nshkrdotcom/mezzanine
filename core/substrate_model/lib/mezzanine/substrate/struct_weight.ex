defmodule Mezzanine.Substrate.StructWeight do
  @moduledoc "Struct-weight catalog for generic substrate DTOs."

  @category_catalog %{
    Mezzanine.Substrate.OperationContext => %{
      identity: [
        :operation_context_ref,
        :actor_ref,
        :tenant_ref,
        :installation_ref,
        :trace_ref,
        :request_ref,
        :idempotency_key
      ],
      context: [
        :workflow_run_ref,
        :work_item_ref,
        :authority_packet_ref,
        :binding_set_ref,
        :run_binding_snapshot_ref,
        :release_manifest_ref,
        :causation_ref
      ],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.OperationRequest => %{
      identity: [:operation_request_ref, :operation_context_ref],
      dispatch: [:operation_role_ref, :operation_class],
      payload: [:payload, :result_schema_ref],
      authority: [:authority_packet_ref],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.PayloadEnvelope => %{
      identity: [:payload_ref],
      payload: [
        :storage_mode,
        :schema_ref,
        :redaction_ref,
        :data,
        :content_ref,
        :content_hash,
        :byte_size,
        :store_ref,
        :stream_ref,
        :retention_refs
      ],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.ResultEnvelope => %{
      identity: [:result_ref],
      payload: [
        :storage_mode,
        :schema_ref,
        :redaction_ref,
        :data,
        :content_ref,
        :content_hash,
        :byte_size,
        :store_ref,
        :stream_ref,
        :retention_refs
      ],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.ResolvedOperationPlan => %{
      identity: [:operation_plan_ref, :operation_context_ref],
      dispatch: [
        :binding_ref,
        :manifest_ref,
        :operation_ref,
        :operation_class,
        :adapter_ref,
        :side_effect_class,
        :lane_policy_ref
      ],
      authority: [:credential_scope_ref, :authority_packet_ref],
      payload: [:input_schema_ref, :output_schema_ref],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.GovernedInvocationEnvelope => %{
      identity: [
        :invocation_ref,
        :operation_context_ref,
        :tenant_ref,
        :installation_ref,
        :trace_ref,
        :idempotency_key
      ],
      dispatch: [:operation_plan],
      payload: [:payload],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.ExecutionInstruction => %{
      identity: [:instruction_ref, :invocation_ref, :operation_context_ref],
      dispatch: [:execution_target_ref, :operation_ref, :timeout_ref, :retry_policy_ref],
      payload: [:payload],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.OperationReceipt => %{
      identity: [:receipt_ref, :operation_context_ref, :operation_plan_ref, :trace_ref],
      result: [:status, :started_at, :completed_at, :result],
      lineage: [:lineage_event_refs],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.OperationLineageEvent => %{
      identity: [:event_ref, :operation_context_ref, :trace_ref],
      lineage: [:event_kind, :occurred_at, :predecessor_refs],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.OperationGroupReceipt => %{
      identity: [:group_receipt_ref, :operation_context_ref],
      result: [:receipt_refs, :status],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.WorkflowRun => %{
      identity: [:workflow_run_ref, :operation_context_ref, :work_item_ref],
      dispatch: [:operation_graph_ref],
      result: [:state],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.OperationDependency => %{
      identity: [:dependency_ref, :from_node_ref, :to_node_ref],
      dispatch: [:relation, :completion_policy, :failure_policy],
      metadata: [:metadata]
    },
    Mezzanine.Substrate.OperationGraph => %{
      identity: [:graph_ref],
      dispatch: [:nodes, :dependencies],
      metadata: [:metadata]
    }
  }

  @spec catalog() :: [map()]
  def catalog do
    @category_catalog
    |> Enum.map(fn {module, categories} ->
      fields = module.fields()

      %{
        module: module,
        enforced_count: length(module.required_fields()),
        total_count: length(fields),
        categories: categories,
        decision: :accepted
      }
    end)
    |> Enum.sort_by(&Atom.to_string(&1.module))
  end
end
