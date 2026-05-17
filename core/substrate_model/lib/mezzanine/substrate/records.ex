defmodule Mezzanine.Substrate.OperationContext do
  @moduledoc "Durable context snapshot for an admitted operation."
  use Mezzanine.Substrate.StructSupport,
    required: [
      :operation_context_ref,
      :actor_ref,
      :tenant_ref,
      :installation_ref,
      :trace_ref,
      :request_ref,
      :idempotency_key
    ],
    optional: [
      :workflow_run_ref,
      :work_item_ref,
      :authority_packet_ref,
      :binding_set_ref,
      :run_binding_snapshot_ref,
      :release_manifest_ref,
      :causation_ref,
      metadata: %{}
    ]
end

defmodule Mezzanine.Substrate.SourceItem do
  @moduledoc "Generic admitted source item."
  use Mezzanine.Substrate.StructSupport,
    required: [
      :source_item_ref,
      :operation_context_ref,
      :source_role_ref,
      :subject_ref,
      :payload
    ],
    optional: [state: :admitted, external_object_refs: [], metadata: %{}]
end

defmodule Mezzanine.Substrate.WorkTarget do
  @moduledoc "Generic work target reference."
  use Mezzanine.Substrate.StructSupport,
    required: [:work_target_ref, :operation_context_ref, :target_kind, :subject_ref],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.WorkItem do
  @moduledoc "Product/operator projection envelope for a workflow run."
  use Mezzanine.Substrate.StructSupport,
    required: [:work_item_ref, :operation_context_ref, :workflow_run_ref, :state],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.Artifact do
  @moduledoc "Generic produced artifact record."
  use Mezzanine.Substrate.StructSupport,
    required: [:artifact_ref, :operation_context_ref, :artifact_kind, :payload],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.Command do
  @moduledoc "Generic admitted command."
  use Mezzanine.Substrate.StructSupport,
    required: [:command_ref, :operation_context_ref, :command_kind, :payload],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.OperationRequest do
  @moduledoc "Generic operation request before binding resolution."
  use Mezzanine.Substrate.StructSupport,
    required: [
      :operation_request_ref,
      :operation_context_ref,
      :operation_role_ref,
      :operation_class,
      :payload
    ],
    optional: [:result_schema_ref, :authority_packet_ref, state: :requested, metadata: %{}]
end

defmodule Mezzanine.Substrate.ResolvedOperationPlan do
  @moduledoc "Captured same-request dispatch facts."
  use Mezzanine.Substrate.StructSupport,
    required: [
      :operation_plan_ref,
      :operation_context_ref,
      :binding_ref,
      :manifest_ref,
      :operation_ref,
      :operation_class,
      :adapter_ref,
      :credential_scope_ref,
      :side_effect_class,
      :input_schema_ref
    ],
    optional: [:output_schema_ref, :lane_policy_ref, :authority_packet_ref, metadata: %{}]
end

defmodule Mezzanine.Substrate.GovernedInvocationEnvelope do
  @moduledoc "Mezzanine to lower governed invocation envelope."
  use Mezzanine.Substrate.StructSupport,
    required: [
      :invocation_ref,
      :operation_context_ref,
      :tenant_ref,
      :installation_ref,
      :trace_ref,
      :idempotency_key,
      :operation_plan,
      :payload
    ],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.ExecutionInstruction do
  @moduledoc "Lower execution instruction."
  use Mezzanine.Substrate.StructSupport,
    required: [
      :instruction_ref,
      :invocation_ref,
      :operation_context_ref,
      :execution_target_ref,
      :operation_ref,
      :payload
    ],
    optional: [:timeout_ref, :retry_policy_ref, metadata: %{}]
end

defmodule Mezzanine.Substrate.OperationReceipt do
  @moduledoc "Compact operation receipt."
  use Mezzanine.Substrate.StructSupport,
    required: [
      :receipt_ref,
      :operation_context_ref,
      :operation_plan_ref,
      :trace_ref,
      :status,
      :started_at,
      :completed_at,
      :result
    ],
    optional: [lineage_event_refs: [], metadata: %{}]
end

defmodule Mezzanine.Substrate.OperationGroupReceipt do
  @moduledoc "Receipt summary for an operation group."
  use Mezzanine.Substrate.StructSupport,
    required: [:group_receipt_ref, :operation_context_ref, :receipt_refs, :status],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.OperationLineageEvent do
  @moduledoc "Append-only operation lineage fact."
  use Mezzanine.Substrate.StructSupport,
    required: [
      :event_ref,
      :operation_context_ref,
      :trace_ref,
      :event_kind,
      :occurred_at,
      :predecessor_refs
    ],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.OperationDisposition do
  @moduledoc "Derived disposition for product/operator policy."
  use Mezzanine.Substrate.StructSupport,
    required: [:disposition_ref, :receipt_ref, :disposition],
    optional: [reason_ref: nil, metadata: %{}]
end

defmodule Mezzanine.Substrate.ReviewCase do
  @moduledoc "Generic review gate state."
  use Mezzanine.Substrate.StructSupport,
    required: [:review_ref, :operation_context_ref, :subject_ref, :state],
    optional: [required_decisions: 1, decisions: [], metadata: %{}]
end

defmodule Mezzanine.Substrate.EvidenceRecord do
  @moduledoc "Generic evidence record."
  use Mezzanine.Substrate.StructSupport,
    required: [:evidence_ref, :operation_context_ref, :subject_ref, :receipt_ref, :payload],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.ProjectionRef do
  @moduledoc "Generic projection reference."
  use Mezzanine.Substrate.StructSupport,
    required: [:projection_ref, :operation_context_ref, :subject_ref, :projection_kind],
    optional: [metadata: %{}]
end

defmodule Mezzanine.Substrate.WorkflowRun do
  @moduledoc "Generic workflow run state."
  use Mezzanine.Substrate.StructSupport,
    required: [:workflow_run_ref, :operation_context_ref, :work_item_ref, :state],
    optional: [:operation_graph_ref, metadata: %{}]
end
