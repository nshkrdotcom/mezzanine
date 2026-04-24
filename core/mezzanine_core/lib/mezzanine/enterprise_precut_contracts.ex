defmodule Mezzanine.EnterprisePrecutSupport do
  @moduledoc false

  @spec build(module(), String.t(), [atom()], [atom()], map() | keyword(), keyword()) ::
          {:ok, struct()} | {:error, term()}
  def build(module, contract_name, fields, required_fields, attrs, opts \\ []) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         [] <- missing_required_fields(attrs, required_fields),
         :ok <- validate_maps(attrs, Keyword.get(opts, :map_fields, [])),
         :ok <- validate_lists(attrs, Keyword.get(opts, :list_fields, [])),
         :ok <- validate_non_neg_integers(attrs, Keyword.get(opts, :non_neg_integer_fields, [])) do
      {:ok, struct(module, attrs |> Map.take(fields) |> Map.put(:contract_name, contract_name))}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}

  defp normalize_attrs(attrs) when is_map(attrs) do
    if Map.has_key?(attrs, :__struct__), do: {:ok, Map.from_struct(attrs)}, else: {:ok, attrs}
  end

  defp normalize_attrs(_attrs), do: {:error, :invalid_attrs}

  defp missing_required_fields(attrs, required_fields) do
    Enum.reject(required_fields, &present?(Map.get(attrs, &1)))
  end

  defp validate_maps(attrs, fields) do
    if Enum.all?(fields, &is_map(Map.get(attrs, &1, %{}))) do
      :ok
    else
      {:error, :invalid_map_field}
    end
  end

  defp validate_lists(attrs, fields) do
    if Enum.all?(fields, &is_list(Map.get(attrs, &1, []))) do
      :ok
    else
      {:error, :invalid_list_field}
    end
  end

  defp validate_non_neg_integers(attrs, fields) do
    if Enum.all?(fields, &(is_integer(Map.get(attrs, &1)) and Map.get(attrs, &1) >= 0)) do
      :ok
    else
      {:error, :invalid_non_negative_integer_field}
    end
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_list(value), do: value != []
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value), do: not is_nil(value)
end

defmodule Mezzanine.CommandReceipt do
  @moduledoc "Durable command acceptance or denial receipt contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :command_id,
    :command_name,
    :command_version,
    :tenant_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :resource_path_hash,
    :trace_id,
    :correlation_id,
    :causation_id,
    :idempotency_key,
    :dedupe_scope,
    :authority_packet_ref,
    :permission_decision_ref,
    :payload_hash,
    :payload_ref,
    :status,
    :rejection_class
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.CommandReceipt.v1",
        @fields,
        [
          :command_id,
          :command_name,
          :command_version,
          :tenant_ref,
          :resource_ref,
          :trace_id,
          :idempotency_key,
          :authority_packet_ref,
          :permission_decision_ref,
          :status
        ],
        attrs
      )
end

defmodule Mezzanine.WorkflowRef do
  @moduledoc "Public-safe workflow identity and operator lookup contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :resource_ref,
    :subject_ref,
    :workflow_type,
    :workflow_id,
    :workflow_run_id,
    :workflow_version,
    :starter_command_id,
    :trace_id,
    :status,
    :search_attributes,
    :release_manifest_version
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowRef.v1",
        @fields,
        [
          :tenant_ref,
          :resource_ref,
          :subject_ref,
          :workflow_type,
          :workflow_id,
          :workflow_version,
          :starter_command_id,
          :trace_id,
          :release_manifest_version
        ],
        attrs,
        map_fields: [:search_attributes]
      )
end

defmodule Mezzanine.WorkflowStartOutboxPayload do
  @moduledoc "Committed local outbox payload that starts Temporal after database commit."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :outbox_id,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :command_envelope_ref,
    :command_receipt_ref,
    :command_id,
    :workflow_type,
    :workflow_id,
    :workflow_version,
    :workflow_input_version,
    :workflow_input_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :canonical_idempotency_key,
    :client_retry_key,
    :platform_envelope_idempotency_key,
    :causation_id,
    :idempotency_key,
    :idempotency_correlation,
    :dedupe_scope,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :payload_hash,
    :payload_ref,
    :dispatch_state,
    :retry_count,
    :last_error_class,
    :workflow_run_id,
    :started_at,
    :available_at,
    :oban_job_ref
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowStartOutboxPayload.v1",
        @fields,
        [
          :outbox_id,
          :tenant_ref,
          :installation_ref,
          :principal_ref,
          :resource_ref,
          :command_receipt_ref,
          :command_id,
          :workflow_type,
          :workflow_id,
          :workflow_version,
          :workflow_input_version,
          :workflow_input_ref,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :dedupe_scope,
          :trace_id,
          :correlation_id,
          :release_manifest_ref,
          :payload_hash,
          :dispatch_state
        ],
        attrs
      )
end

defmodule Mezzanine.WorkflowSignalReceipt do
  @moduledoc "Authorized workflow signal lifecycle receipt."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :operator_ref,
    :resource_ref,
    :signal_id,
    :workflow_id,
    :workflow_run_id,
    :signal_name,
    :signal_version,
    :signal_sequence,
    :command_id,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :payload_hash,
    :payload_ref,
    :authority_state,
    :local_state,
    :dispatch_state,
    :workflow_effect_state,
    :projection_state,
    :delivery_status,
    :dispatch_attempt_count,
    :dispatch_error_class,
    :workflow_ack_event_ref,
    :workflow_acknowledged_at,
    :staleness_started_at
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowSignalReceipt.v1",
        @fields,
        [
          :tenant_ref,
          :installation_ref,
          :resource_ref,
          :signal_id,
          :workflow_id,
          :signal_name,
          :signal_version,
          :command_id,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :release_manifest_ref,
          :authority_state,
          :local_state,
          :dispatch_state,
          :workflow_effect_state,
          :projection_state
        ],
        attrs
      )
end

defmodule Mezzanine.WorkflowExecutionLifecycleInput do
  @moduledoc "Execution lifecycle workflow input contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :subject_ref,
    :workflow_id,
    :workflow_run_id,
    :workflow_type,
    :workflow_version,
    :command_id,
    :command_receipt_ref,
    :workflow_input_ref,
    :lower_submission_ref,
    :lower_idempotency_key,
    :activity_call_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :retry_policy,
    :terminal_policy,
    :routing_facts
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowExecutionLifecycleInput.v1",
        @fields,
        [
          :tenant_ref,
          :installation_ref,
          :principal_ref,
          :resource_ref,
          :subject_ref,
          :workflow_id,
          :workflow_type,
          :workflow_version,
          :command_id,
          :command_receipt_ref,
          :workflow_input_ref,
          :lower_submission_ref,
          :lower_idempotency_key,
          :activity_call_ref,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :release_manifest_ref,
          :retry_policy,
          :terminal_policy,
          :routing_facts
        ],
        attrs,
        map_fields: [:retry_policy, :routing_facts]
      )
end

defmodule Mezzanine.WorkflowReceiptSignal do
  @moduledoc "Tenant-scoped lower receipt signal delivered to an execution workflow."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :workflow_id,
    :workflow_run_id,
    :signal_id,
    :signal_name,
    :signal_version,
    :lower_receipt_ref,
    :lower_run_ref,
    :lower_attempt_ref,
    :lower_event_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :receipt_state,
    :terminal?,
    :routing_facts
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowReceiptSignal.v1",
        @fields,
        [
          :tenant_ref,
          :installation_ref,
          :resource_ref,
          :workflow_id,
          :signal_id,
          :signal_name,
          :signal_version,
          :lower_receipt_ref,
          :lower_run_ref,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :release_manifest_ref,
          :receipt_state,
          :terminal?,
          :routing_facts
        ],
        attrs,
        map_fields: [:routing_facts]
      )
end

defmodule Mezzanine.WorkflowTerminalReceiptPolicy do
  @moduledoc "Policy record for lower receipts that arrive after terminal workflow state."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :workflow_id,
    :workflow_run_id,
    :terminal_state,
    :terminal_event_ref,
    :late_receipt_ref,
    :policy_result,
    :incident_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowTerminalReceiptPolicy.v1",
        @fields,
        [
          :tenant_ref,
          :installation_ref,
          :resource_ref,
          :workflow_id,
          :terminal_state,
          :terminal_event_ref,
          :late_receipt_ref,
          :policy_result,
          :incident_ref,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :release_manifest_ref
        ],
        attrs
      )
end

defmodule Mezzanine.WorkflowDecisionTimer do
  @moduledoc "Durable workflow timer contract for human or operator decision expiry."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :subject_ref,
    :workflow_id,
    :workflow_run_id,
    :decision_id,
    :decision_kind,
    :timer_id,
    :timer_version,
    :timer_duration_ms,
    :expires_at,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :workflow_history_ref,
    :projection_ref,
    :timer_state
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowDecisionTimer.v1",
        @fields,
        [
          :tenant_ref,
          :installation_ref,
          :system_actor_ref,
          :resource_ref,
          :subject_ref,
          :workflow_id,
          :decision_id,
          :decision_kind,
          :timer_id,
          :timer_version,
          :timer_duration_ms,
          :expires_at,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :release_manifest_ref,
          :workflow_history_ref,
          :projection_ref,
          :timer_state
        ],
        attrs,
        non_neg_integer_fields: [:timer_duration_ms]
      )
end

defmodule Mezzanine.OperatorWorkflowSignal do
  @moduledoc "Authorized operator workflow signal contract for cancel, pause, resume, retry, and replan."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :operator_ref,
    :resource_ref,
    :workflow_id,
    :workflow_run_id,
    :signal_id,
    :signal_name,
    :signal_version,
    :signal_sequence,
    :signal_effect,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :acknowledgement_ttl_ms,
    :reason,
    :payload_hash,
    :payload_ref
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.OperatorWorkflowSignal.v1",
        @fields,
        [
          :tenant_ref,
          :installation_ref,
          :principal_ref,
          :operator_ref,
          :resource_ref,
          :workflow_id,
          :signal_id,
          :signal_name,
          :signal_version,
          :signal_sequence,
          :signal_effect,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :release_manifest_ref,
          :acknowledgement_ttl_ms,
          :payload_hash,
          :payload_ref
        ],
        attrs,
        non_neg_integer_fields: [:signal_sequence, :acknowledgement_ttl_ms]
      )
end

defmodule Mezzanine.WorkflowSignalOutboxRow do
  @moduledoc "Local transactional signal outbox row committed before Temporal delivery."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :outbox_id,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :operator_ref,
    :resource_ref,
    :signal_id,
    :workflow_id,
    :workflow_run_id,
    :signal_name,
    :signal_version,
    :signal_sequence,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :dispatch_state,
    :workflow_effect_state,
    :projection_state,
    :available_at,
    :dispatch_attempt_count,
    :last_error_class,
    :oban_job_ref
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowSignalOutboxRow.v1",
        @fields,
        [
          :outbox_id,
          :tenant_ref,
          :installation_ref,
          :principal_ref,
          :operator_ref,
          :resource_ref,
          :signal_id,
          :workflow_id,
          :signal_name,
          :signal_version,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :release_manifest_ref,
          :dispatch_state,
          :workflow_effect_state,
          :projection_state,
          :available_at,
          :dispatch_attempt_count
        ],
        attrs,
        non_neg_integer_fields: [:dispatch_attempt_count]
      )
end

defmodule Mezzanine.WorkflowSignalAcknowledgement do
  @moduledoc "Workflow-emitted acknowledgement fact proving that a signal handler observed the signal."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :operator_ref,
    :resource_ref,
    :workflow_id,
    :workflow_run_id,
    :signal_id,
    :signal_name,
    :signal_version,
    :signal_sequence,
    :signal_effect,
    :workflow_effect_state,
    :workflow_event_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :acknowledged_at,
    :failure_class
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowSignalAcknowledgement.v1",
        @fields,
        [
          :tenant_ref,
          :installation_ref,
          :resource_ref,
          :workflow_id,
          :signal_id,
          :signal_name,
          :signal_version,
          :signal_sequence,
          :signal_effect,
          :workflow_effect_state,
          :workflow_event_ref,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :correlation_id,
          :release_manifest_ref,
          :acknowledged_at
        ],
        attrs,
        non_neg_integer_fields: [:signal_sequence]
      )
end

defmodule Mezzanine.EventFact do
  @moduledoc "Append-only event fact envelope contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :event_fact_id,
    :tenant_ref,
    :fact_kind,
    :producer_repo,
    :resource_ref,
    :subject_ref,
    :workflow_ref,
    :lower_ref,
    :semantic_ref,
    :trace_id,
    :causation_id,
    :payload_hash,
    :payload_ref,
    :redaction_posture
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.EventFact.v1",
        @fields,
        [:event_fact_id, :tenant_ref, :fact_kind, :producer_repo, :resource_ref, :trace_id],
        attrs
      )
end

defmodule Mezzanine.ProjectionSnapshot do
  @moduledoc "Projection/snapshot envelope contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :projection_id,
    :tenant_ref,
    :projection_kind,
    :owner_repo,
    :source_position,
    :staleness_class,
    :trace_id
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.ProjectionSnapshot.v1",
        @fields,
        [
          :projection_id,
          :tenant_ref,
          :projection_kind,
          :owner_repo,
          :source_position,
          :staleness_class,
          :trace_id
        ],
        attrs
      )
end

defmodule Mezzanine.ReviewTask do
  @moduledoc "Human/operator review requirement contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :review_task_id,
    :tenant_ref,
    :resource_ref,
    :subject_ref,
    :workflow_ref,
    :requested_by_ref,
    :required_action,
    :authority_context_ref,
    :status,
    :trace_id
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.ReviewTask.v1",
        @fields,
        [
          :review_task_id,
          :tenant_ref,
          :resource_ref,
          :requested_by_ref,
          :required_action,
          :authority_context_ref,
          :status,
          :trace_id
        ],
        attrs
      )
end

defmodule Mezzanine.ActivityCallRef do
  @moduledoc "Workflow activity call reference contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :activity_call_id,
    :activity_name,
    :activity_version,
    :workflow_ref,
    :tenant_ref,
    :resource_ref,
    :idempotency_key,
    :trace_id,
    :owner_repo,
    :timeout_policy,
    :retry_policy
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.ActivityCallRef.v1",
        @fields,
        [
          :activity_call_id,
          :activity_name,
          :activity_version,
          :workflow_ref,
          :tenant_ref,
          :resource_ref,
          :idempotency_key,
          :trace_id,
          :owner_repo,
          :timeout_policy,
          :retry_policy
        ],
        attrs
      )
end

defmodule Mezzanine.ActivityResult do
  @moduledoc "Compact workflow-visible activity result contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :activity_call_id,
    :tenant_ref,
    :resource_ref,
    :workflow_ref,
    :lower_ref,
    :semantic_ref,
    :routing_facts,
    :failure_class,
    :retry_class,
    :trace_id
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.ActivityResult.v1",
        @fields,
        [:activity_call_id, :tenant_ref, :resource_ref, :workflow_ref, :trace_id],
        attrs,
        map_fields: [:routing_facts]
      )
end

defmodule Mezzanine.ActivityLeaseScopeRequest do
  @moduledoc "Lease/grant broker request contract for lower activity authorization."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :tenant_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :resource_path,
    :authority_packet_ref,
    :permission_decision_ref,
    :policy_revision,
    :lease_epoch,
    :revocation_epoch,
    :activity_type,
    :activity_id,
    :workflow_ref,
    :lower_scope_ref,
    :requested_capabilities,
    :idempotency_key,
    :trace_id,
    :deadline
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.ActivityLeaseScopeRequest.v1",
        @fields,
        [
          :tenant_ref,
          :resource_ref,
          :authority_packet_ref,
          :permission_decision_ref,
          :policy_revision,
          :lease_epoch,
          :revocation_epoch,
          :activity_type,
          :activity_id,
          :workflow_ref,
          :lower_scope_ref,
          :requested_capabilities,
          :idempotency_key,
          :trace_id,
          :deadline
        ],
        attrs,
        list_fields: [:requested_capabilities],
        non_neg_integer_fields: [:lease_epoch, :revocation_epoch]
      )
end

defmodule Mezzanine.ActivityLeaseBundle do
  @moduledoc "Opaque worker-local lease or attach-grant evidence bundle."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :lease_ref,
    :attach_grant_ref,
    :capability_scope_hash,
    :authority_packet_ref,
    :permission_decision_ref,
    :policy_revision,
    :lease_epoch,
    :revocation_epoch,
    :expires_at,
    :max_uses,
    :remaining_uses,
    :cache_status,
    :evidence_ref,
    :failure_class
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.ActivityLeaseBundle.v1",
        @fields,
        [
          :lease_ref,
          :capability_scope_hash,
          :authority_packet_ref,
          :permission_decision_ref,
          :policy_revision,
          :lease_epoch,
          :revocation_epoch,
          :expires_at,
          :max_uses,
          :remaining_uses,
          :cache_status,
          :evidence_ref,
          :failure_class
        ],
        attrs,
        non_neg_integer_fields: [:lease_epoch, :revocation_epoch, :max_uses, :remaining_uses]
      )
end

defmodule Mezzanine.IncidentBundle do
  @moduledoc "Trace-rooted incident reconstruction bundle contract."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :incident_bundle_id,
    :tenant_ref,
    :trace_id,
    :command_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :workflow_ref,
    :activity_call_refs,
    :lower_refs,
    :semantic_refs,
    :projection_refs,
    :release_manifest_version,
    :proof_artifact_path
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.IncidentBundle.v1",
        @fields,
        [
          :incident_bundle_id,
          :tenant_ref,
          :trace_id,
          :command_ref,
          :authority_packet_ref,
          :permission_decision_ref,
          :workflow_ref,
          :release_manifest_version,
          :proof_artifact_path
        ],
        attrs,
        list_fields: [:activity_call_refs, :lower_refs, :semantic_refs, :projection_refs]
      )
end

defmodule Mezzanine.WorkflowStartReceipt do
  @moduledoc "Public-safe workflow start receipt."

  alias Mezzanine.EnterprisePrecutSupport

  @fields [
    :contract_name,
    :workflow_ref,
    :workflow_id,
    :workflow_run_id,
    :workflow_type,
    :workflow_version,
    :tenant_ref,
    :resource_ref,
    :command_id,
    :idempotency_key,
    :idempotency_correlation,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :start_state,
    :duplicate?,
    :retry_class,
    :failure_class
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  def new(attrs),
    do:
      EnterprisePrecutSupport.build(
        __MODULE__,
        "Mezzanine.WorkflowStartReceipt.v1",
        @fields,
        [
          :workflow_ref,
          :workflow_id,
          :workflow_type,
          :workflow_version,
          :tenant_ref,
          :resource_ref,
          :command_id,
          :idempotency_key,
          :trace_id,
          :start_state
        ],
        attrs
      )
end

defmodule Mezzanine.WorkflowSignalReceiptResult do
  @moduledoc "Public-safe workflow signal runtime receipt."
  defstruct [:signal_ref, :status, :trace_id, :failure_class]

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.WorkflowQueryResult do
  @moduledoc "Public-safe workflow query result."
  defstruct [:workflow_ref, :query_name, :state_ref, :summary, :trace_id, :failure_class]

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.WorkflowCancelReceipt do
  @moduledoc "Public-safe workflow cancel receipt."
  defstruct [:workflow_ref, :status, :trace_id, :failure_class]

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.WorkflowDescription do
  @moduledoc "Public-safe workflow description."
  defstruct [:workflow_ref, :status, :search_attributes, :trace_id, :failure_class]

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.WorkflowHistoryRef do
  @moduledoc "Claim-check style reference to workflow history evidence."
  defstruct [:workflow_ref, :history_ref, :history_hash, :trace_id]

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.WorkflowRuntime do
  @moduledoc """
  Behaviour for the only Mezzanine-owned Temporal client boundary.
  """

  @callback start_workflow(term()) :: {:ok, Mezzanine.WorkflowStartReceipt.t()} | {:error, term()}
  @callback signal_workflow(term()) ::
              {:ok, Mezzanine.WorkflowSignalReceiptResult.t()} | {:error, term()}
  @callback query_workflow(term()) :: {:ok, Mezzanine.WorkflowQueryResult.t()} | {:error, term()}
  @callback cancel_workflow(term()) ::
              {:ok, Mezzanine.WorkflowCancelReceipt.t()} | {:error, term()}
  @callback describe_workflow(term()) ::
              {:ok, Mezzanine.WorkflowDescription.t()} | {:error, term()}
  @callback fetch_workflow_history_ref(term()) ::
              {:ok, Mezzanine.WorkflowHistoryRef.t()} | {:error, term()}

  @spec start_workflow(term()) :: {:ok, Mezzanine.WorkflowStartReceipt.t()} | {:error, term()}
  def start_workflow(request), do: implementation().start_workflow(request)

  @spec signal_workflow(term()) ::
          {:ok, Mezzanine.WorkflowSignalReceiptResult.t()} | {:error, term()}
  def signal_workflow(request), do: implementation().signal_workflow(request)

  @spec query_workflow(term()) :: {:ok, Mezzanine.WorkflowQueryResult.t()} | {:error, term()}
  def query_workflow(request), do: implementation().query_workflow(request)

  @spec cancel_workflow(term()) ::
          {:ok, Mezzanine.WorkflowCancelReceipt.t()} | {:error, term()}
  def cancel_workflow(request), do: implementation().cancel_workflow(request)

  @spec describe_workflow(term()) ::
          {:ok, Mezzanine.WorkflowDescription.t()} | {:error, term()}
  def describe_workflow(request), do: implementation().describe_workflow(request)

  @spec fetch_workflow_history_ref(term()) ::
          {:ok, Mezzanine.WorkflowHistoryRef.t()} | {:error, term()}
  def fetch_workflow_history_ref(request),
    do: implementation().fetch_workflow_history_ref(request)

  defp implementation do
    Application.get_env(
      :mezzanine_core,
      :workflow_runtime_impl,
      Mezzanine.WorkflowRuntime.Unconfigured
    )
  end
end

defmodule Mezzanine.WorkflowRuntime.Unconfigured do
  @moduledoc false

  @behaviour Mezzanine.WorkflowRuntime

  @impl true
  def start_workflow(_request), do: {:error, :workflow_runtime_unconfigured}

  @impl true
  def signal_workflow(_request), do: {:error, :workflow_runtime_unconfigured}

  @impl true
  def query_workflow(_request), do: {:error, :workflow_runtime_unconfigured}

  @impl true
  def cancel_workflow(_request), do: {:error, :workflow_runtime_unconfigured}

  @impl true
  def describe_workflow(_request), do: {:error, :workflow_runtime_unconfigured}

  @impl true
  def fetch_workflow_history_ref(_request), do: {:error, :workflow_runtime_unconfigured}
end

defmodule Mezzanine.ActivityLeaseBroker do
  @moduledoc """
  Behaviour for worker-local lower lease and attach-grant bundle brokering.
  """

  @cache_key_version 1
  @default_max_uses 5
  @failure_classes [
    :lease_denied,
    :lease_stale,
    :lease_revoked,
    :lease_expired,
    :lease_scope_mismatch,
    :lease_epoch_mismatch,
    :lease_cache_unavailable,
    :lease_mint_unavailable_retryable,
    :lease_mint_failed_terminal
  ]

  @callback acquire(Mezzanine.ActivityLeaseScopeRequest.t()) ::
              {:ok, Mezzanine.ActivityLeaseBundle.t()} | {:error, term()}
  @callback refresh(Mezzanine.ActivityLeaseScopeRequest.t(), term()) ::
              {:ok, Mezzanine.ActivityLeaseBundle.t()} | {:error, term()}
  @callback revoke_seen(term()) :: :ok | {:error, term()}
  @callback invalidate(term()) :: :ok | {:error, term()}

  @doc "Version of the opaque worker-local lease bundle cache key."
  @spec cache_key_version() :: pos_integer()
  def cache_key_version, do: @cache_key_version

  @doc "Terminal and retryable failure classes emitted by the activity lease broker."
  @spec failure_classes() :: [atom()]
  def failure_classes, do: @failure_classes

  @doc """
  Acquire an authority bundle for an activity scope.

  The cache is intentionally process-local so activity workers can reuse scoped
  authority evidence without entering deterministic workflow history.
  """
  @spec acquire(Mezzanine.ActivityLeaseScopeRequest.t() | map() | keyword()) ::
          {:ok, Mezzanine.ActivityLeaseBundle.t()} | {:error, term()}
  def acquire(request) do
    with {:ok, request} <- normalize_request(request),
         :ok <- deny_if_revoked(request) do
      acquire_from_cache(request)
    end
  end

  @doc "Refresh a stale bundle by invalidating the prior scope and minting a new evidence bundle."
  @spec refresh(Mezzanine.ActivityLeaseScopeRequest.t() | map() | keyword(), term()) ::
          {:ok, Mezzanine.ActivityLeaseBundle.t()} | {:error, term()}
  def refresh(request, stale_bundle_ref) do
    _ = invalidate(stale_bundle_ref)

    with {:ok, request} <- normalize_request(request),
         :ok <- deny_if_revoked(request) do
      mint_and_store(request, "refreshed")
    end
  end

  @doc "Record a revocation event and evict matching worker-local cache entries."
  @spec revoke_seen(map() | keyword()) :: :ok | {:error, term()}
  def revoke_seen(event) do
    event = normalize_map(event)
    key = revocation_key(event)
    epoch = integer_value(event, :revocation_epoch, 0)

    revocations =
      process_get(:revocations, %{})
      |> Map.update(key, epoch, &max(&1, epoch))

    Process.put(process_key(:revocations), revocations)

    invalidated =
      cache()
      |> Enum.filter(fn {_cache_key, %{request: request}} ->
        revocation_match?(request, event)
      end)
      |> Enum.map(fn {cache_key, _entry} -> cache_key end)

    Enum.each(invalidated, &delete_cache_key/1)
    metric(:revocation_invalidations, length(invalidated))
    :ok
  end

  @doc "Invalidate one cache key, one cache epoch, or all cache state for the current worker."
  @spec invalidate(term()) :: :ok | {:error, term()}
  def invalidate(:all) do
    Process.delete(process_key(:cache))
    :ok
  end

  def invalidate(cache_key) when is_binary(cache_key) do
    delete_cache_key(cache_key)
    :ok
  end

  def invalidate(epoch) when is_integer(epoch) do
    cache()
    |> Enum.filter(fn {_key, %{bundle: bundle}} ->
      bundle.lease_epoch == epoch or bundle.revocation_epoch == epoch
    end)
    |> Enum.each(fn {key, _entry} -> delete_cache_key(key) end)

    :ok
  end

  def invalidate(_cache_key_or_epoch), do: :ok

  @doc "Current process-local broker metrics used by Stack Lab and unit proofs."
  @spec metrics() :: map()
  def metrics do
    metrics = process_get(:metrics, %{})
    hits = Map.get(metrics, :cache_hit_count, 0)
    misses = Map.get(metrics, :cache_miss_count, 0)
    total = hits + misses

    Map.merge(
      %{
        mint_count: 0,
        cache_hit_count: 0,
        cache_miss_count: 0,
        cache_hit_rate: if(total == 0, do: 0.0, else: hits / total),
        revocation_invalidations: 0,
        mint_latency_microseconds: [],
        failure_classes: %{}
      },
      metrics
    )
    |> Map.put(:cache_hit_rate, if(total == 0, do: 0.0, else: hits / total))
  end

  @doc "Reset current worker cache and metrics. Intended for tests and Stack Lab proof setup."
  @spec reset_worker_cache!() :: :ok
  def reset_worker_cache! do
    Process.delete(process_key(:cache))
    Process.delete(process_key(:metrics))
    Process.delete(process_key(:revocations))
    :ok
  end

  @doc "Build the stable opaque cache key for a scope request without exposing lease material."
  @spec cache_key(Mezzanine.ActivityLeaseScopeRequest.t() | map() | keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def cache_key(request) do
    with {:ok, request} <- normalize_request(request) do
      {:ok, cache_key!(request)}
    end
  end

  defp acquire_from_cache(request) do
    key = cache_key!(request)

    case Map.get(cache(), key) do
      %{bundle: bundle} = entry ->
        if reusable?(request, bundle) do
          hit = update_cache_status(bundle, "hit")
          used = %{hit | remaining_uses: max(hit.remaining_uses - 1, 0)}
          put_cache_key(key, %{entry | bundle: used})
          metric(:cache_hit_count, 1)
          {:ok, used}
        else
          delete_cache_key(key)
          metric(:cache_miss_count, 1)
          mint_and_store(request, "miss")
        end

      nil ->
        metric(:cache_miss_count, 1)
        mint_and_store(request, "miss")
    end
  end

  defp mint_and_store(request, cache_status) do
    started = System.monotonic_time(:microsecond)
    key = cache_key!(request)
    capability_hash = capability_scope_hash(request)

    with {:ok, bundle} <-
           Mezzanine.ActivityLeaseBundle.new(%{
             lease_ref: "lease://#{capability_hash}",
             attach_grant_ref: attach_grant_ref(request, capability_hash),
             capability_scope_hash: capability_hash,
             authority_packet_ref: request.authority_packet_ref,
             permission_decision_ref: request.permission_decision_ref,
             policy_revision: request.policy_revision,
             lease_epoch: request.lease_epoch,
             revocation_epoch: request.revocation_epoch,
             expires_at: request.deadline,
             max_uses: @default_max_uses,
             remaining_uses: @default_max_uses - 1,
             cache_status: cache_status,
             evidence_ref: "evidence://activity-lease/#{capability_hash}",
             failure_class: "none"
           }) do
      put_cache_key(key, %{request: request, bundle: bundle})
      metric(:mint_count, 1)
      metric(:mint_latency_microseconds, System.monotonic_time(:microsecond) - started)
      {:ok, bundle}
    end
  end

  defp normalize_request(%Mezzanine.ActivityLeaseScopeRequest{} = request), do: {:ok, request}

  defp normalize_request(request), do: Mezzanine.ActivityLeaseScopeRequest.new(request)

  defp normalize_map(value) when is_list(value), do: Map.new(value)
  defp normalize_map(value) when is_map(value), do: value
  defp normalize_map(_value), do: %{}

  defp reusable?(request, bundle) do
    bundle.remaining_uses > 0 and
      bundle.lease_epoch == request.lease_epoch and
      bundle.revocation_epoch == request.revocation_epoch and
      not expired?(bundle.expires_at) and
      deny_if_revoked(request) == :ok
  end

  defp expired?(nil), do: false

  defp expired?(expires_at) when is_binary(expires_at) do
    case DateTime.from_iso8601(expires_at) do
      {:ok, deadline, _offset} -> DateTime.compare(deadline, DateTime.utc_now()) == :lt
      _invalid -> false
    end
  end

  defp expired?(_expires_at), do: false

  defp deny_if_revoked(request) do
    seen_epoch =
      process_get(:revocations, %{})
      |> Map.get(revocation_key(request), 0)

    if request.revocation_epoch < seen_epoch do
      metric_failure(:lease_revoked)
      {:error, {:activity_lease_denied, :lease_revoked}}
    else
      :ok
    end
  end

  defp update_cache_status(bundle, cache_status), do: %{bundle | cache_status: cache_status}

  defp cache_key!(request) do
    key_material = %{
      cache_key_version: @cache_key_version,
      tenant_ref: request.tenant_ref,
      actor_ref: request.principal_ref || request.system_actor_ref,
      resource_ref: request.resource_ref,
      resource_path: request.resource_path,
      authority_packet_ref: request.authority_packet_ref,
      permission_decision_ref: request.permission_decision_ref,
      policy_revision: request.policy_revision,
      lease_epoch: request.lease_epoch,
      revocation_epoch: request.revocation_epoch,
      activity_type: request.activity_type,
      lower_scope_ref: request.lower_scope_ref,
      requested_capabilities: Enum.sort(request.requested_capabilities)
    }

    "activity-lease-cache:v#{@cache_key_version}:#{hash(key_material)}"
  end

  defp capability_scope_hash(request) do
    %{
      tenant_ref: request.tenant_ref,
      actor_ref: request.principal_ref || request.system_actor_ref,
      resource_ref: request.resource_ref,
      lower_scope_ref: request.lower_scope_ref,
      requested_capabilities: Enum.sort(request.requested_capabilities),
      lease_epoch: request.lease_epoch,
      revocation_epoch: request.revocation_epoch
    }
    |> hash()
  end

  defp attach_grant_ref(request, capability_hash) do
    if Enum.any?(request.requested_capabilities, &String.contains?(to_string(&1), "attach")) do
      "attach-grant://#{capability_hash}"
    end
  end

  defp hash(term) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(term))
    |> Base.encode16(case: :lower)
  end

  defp revocation_key(source) do
    source = normalize_map(source)

    {
      value(source, :tenant_ref),
      value(source, :resource_ref),
      value(source, :lower_scope_ref)
    }
  end

  defp revocation_match?(request, event) do
    revocation_key(request) == revocation_key(event) and
      request.revocation_epoch <=
        integer_value(event, :revocation_epoch, request.revocation_epoch)
  end

  defp value(%{__struct__: _} = source, field), do: source |> Map.from_struct() |> value(field)

  defp value(source, field) when is_map(source),
    do: Map.get(source, field) || Map.get(source, Atom.to_string(field))

  defp integer_value(source, field, default) do
    case value(source, field) do
      value when is_integer(value) ->
        value

      value when is_binary(value) ->
        case Integer.parse(value) do
          {integer, ""} -> integer
          _invalid -> default
        end

      _other ->
        default
    end
  end

  defp cache, do: process_get(:cache, %{})

  defp put_cache_key(key, entry) do
    Process.put(process_key(:cache), Map.put(cache(), key, entry))
  end

  defp delete_cache_key(key) do
    Process.put(process_key(:cache), Map.delete(cache(), key))
  end

  defp metric(:mint_latency_microseconds, value) do
    metrics = process_get(:metrics, %{})
    latencies = [value | Map.get(metrics, :mint_latency_microseconds, [])]
    Process.put(process_key(:metrics), Map.put(metrics, :mint_latency_microseconds, latencies))
  end

  defp metric(name, value) do
    metrics = process_get(:metrics, %{})
    Process.put(process_key(:metrics), Map.update(metrics, name, value, &(&1 + value)))
  end

  defp metric_failure(failure_class) do
    metrics = process_get(:metrics, %{})
    failures = Map.update(Map.get(metrics, :failure_classes, %{}), failure_class, 1, &(&1 + 1))
    Process.put(process_key(:metrics), Map.put(metrics, :failure_classes, failures))
  end

  defp process_get(name, default), do: Process.get(process_key(name), default)

  defp process_key(name), do: {__MODULE__, name}
end
