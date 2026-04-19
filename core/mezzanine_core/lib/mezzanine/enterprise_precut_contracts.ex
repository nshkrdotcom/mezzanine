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
    :idempotency_key,
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
    :signal_id,
    :workflow_id,
    :workflow_run_id,
    :signal_name,
    :signal_version,
    :command_id,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
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
          :signal_id,
          :workflow_id,
          :signal_name,
          :signal_version,
          :command_id,
          :authority_packet_ref,
          :permission_decision_ref,
          :idempotency_key,
          :trace_id,
          :authority_state,
          :local_state,
          :dispatch_state,
          :workflow_effect_state,
          :projection_state
        ],
        attrs
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

  @callback acquire(Mezzanine.ActivityLeaseScopeRequest.t()) ::
              {:ok, Mezzanine.ActivityLeaseBundle.t()} | {:error, term()}
  @callback refresh(Mezzanine.ActivityLeaseScopeRequest.t(), term()) ::
              {:ok, Mezzanine.ActivityLeaseBundle.t()} | {:error, term()}
  @callback revoke_seen(term()) :: :ok | {:error, term()}
  @callback invalidate(term()) :: :ok | {:error, term()}
end
