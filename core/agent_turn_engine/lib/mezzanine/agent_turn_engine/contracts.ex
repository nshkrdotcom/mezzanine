defmodule Mezzanine.AgentTurnEngine.AgentTurnLedger do
  @moduledoc """
  Durable sequence scope for one platform agent run or turn group.
  """

  alias Mezzanine.AgentTurnEngine.Validation

  @statuses [:initialized, :running, :pending, :completed, :failed, :cancelled]

  @enforce_keys [
    :ledger_ref,
    :tenant_ref,
    :installation_ref,
    :subject_ref,
    :platform_run_ref,
    :platform_execution_ref,
    :actor_ref,
    :authority_ref,
    :idempotency_key,
    :status,
    :next_seq,
    :last_reduced_seq,
    :last_conversation_seq,
    :last_execution_seq,
    :created_at,
    :updated_at
  ]
  defstruct @enforce_keys ++ [:cursor_ref, :replay_ref, :pending_interaction_ref]

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_lower_leakage(attrs),
         :ok <- Validation.ref(attrs, :ledger_ref, "agent-ledger://"),
         :ok <- Validation.ref(attrs, :tenant_ref, "tenant://"),
         :ok <- Validation.ref(attrs, :installation_ref, "installation://"),
         :ok <- Validation.ref(attrs, :subject_ref, "subject://"),
         :ok <- Validation.ref(attrs, :platform_run_ref, "run://"),
         :ok <- Validation.ref(attrs, :platform_execution_ref, "execution://"),
         :ok <- Validation.ref(attrs, :actor_ref, "actor://"),
         :ok <- Validation.ref(attrs, :authority_ref, "authority://"),
         :ok <- Validation.required_binary(attrs, :idempotency_key),
         :ok <- Validation.one_of(attrs, :status, @statuses),
         :ok <- Validation.positive_integer(attrs, :next_seq),
         :ok <- Validation.non_negative_integer(attrs, :last_reduced_seq),
         :ok <- Validation.non_negative_integer(attrs, :last_conversation_seq),
         :ok <- Validation.non_negative_integer(attrs, :last_execution_seq),
         :ok <- Validation.optional_ref(attrs, :cursor_ref, "agent-cursor://"),
         :ok <- Validation.optional_ref(attrs, :replay_ref, "agent-replay://"),
         :ok <- Validation.optional_ref(attrs, :pending_interaction_ref, "agent-pending://"),
         :ok <- Validation.datetime(attrs, :created_at),
         :ok <- Validation.datetime(attrs, :updated_at) do
      {:ok, struct!(__MODULE__, Validation.take(attrs, __struct__()))}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, ledger} -> ledger
      {:error, reason} -> raise ArgumentError, "invalid agent turn ledger: #{inspect(reason)}"
    end
  end
end

defmodule Mezzanine.AgentTurnEngine.AgentConversationEvent do
  @moduledoc """
  User/operator-visible event class suitable for product timelines.
  """

  alias Mezzanine.AgentTurnEngine.Validation

  @event_types [
    :run_started,
    :user_turn_received,
    :assistant_message_available,
    :pending_review_requested,
    :review_decision_recorded,
    :tool_result_summarized,
    :run_completed,
    :run_failed,
    :run_cancelled
  ]
  @visibilities [:product, :operator, :internal_summary]
  @redaction_classes [:safe, :redacted, :restricted]

  @enforce_keys [
    :event_ref,
    :ledger_ref,
    :seq,
    :event_type,
    :visibility,
    :summary,
    :payload_ref,
    :redaction_class,
    :authority_ref,
    :evidence_refs,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_lower_leakage(attrs),
         :ok <- Validation.ref(attrs, :event_ref, "agent-conv-event://"),
         :ok <- Validation.ref(attrs, :ledger_ref, "agent-ledger://"),
         :ok <- Validation.positive_integer(attrs, :seq),
         :ok <- Validation.one_of(attrs, :event_type, @event_types),
         :ok <- Validation.one_of(attrs, :visibility, @visibilities),
         :ok <- Validation.required_binary(attrs, :summary),
         :ok <- Validation.ref(attrs, :payload_ref, "payload://"),
         :ok <- Validation.one_of(attrs, :redaction_class, @redaction_classes),
         :ok <- Validation.ref(attrs, :authority_ref, "authority://"),
         :ok <- Validation.ref_list(attrs, :evidence_refs, ["evidence://", "receipt://"]),
         :ok <- Validation.datetime(attrs, :occurred_at) do
      {:ok, struct!(__MODULE__, Validation.take(attrs, __struct__()))}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs), do: Validation.new!(__MODULE__, attrs, &new/1)
end

defmodule Mezzanine.AgentTurnEngine.AgentExecutionEvent do
  @moduledoc """
  Internal event class for lower execution, replay, debugging, and receipt reduction.
  """

  alias Mezzanine.AgentTurnEngine.Validation

  @event_types [
    :dispatch_requested,
    :authority_projection_bound,
    :lower_invocation_started,
    :lower_stream_event,
    :tool_call_requested,
    :tool_call_completed,
    :skill_invocation_requested,
    :pending_interaction_opened,
    :runtime_receipt_received,
    :runtime_receipt_reduced,
    :replay_catchup_served,
    :terminal_reduction_completed,
    :failure_classified
  ]
  @sources [:mezzanine, :citadel, :jido, :execution_plane, :aitrace]
  @redaction_classes [:internal, :redacted, :restricted]

  @enforce_keys [
    :event_ref,
    :ledger_ref,
    :seq,
    :event_type,
    :source,
    :idempotency_key,
    :causation_ref,
    :lower_receipt_ref,
    :payload_hash,
    :payload_ref,
    :redaction_class,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_lower_leakage(attrs),
         :ok <- Validation.ref(attrs, :event_ref, "agent-exec-event://"),
         :ok <- Validation.ref(attrs, :ledger_ref, "agent-ledger://"),
         :ok <- Validation.positive_integer(attrs, :seq),
         :ok <- Validation.one_of(attrs, :event_type, @event_types),
         :ok <- Validation.one_of(attrs, :source, @sources),
         :ok <- Validation.required_binary(attrs, :idempotency_key),
         :ok <- Validation.required_binary(attrs, :causation_ref),
         :ok <- Validation.ref(attrs, :lower_receipt_ref, "receipt://"),
         :ok <- Validation.sha256(attrs, :payload_hash),
         :ok <- Validation.ref(attrs, :payload_ref, "payload://"),
         :ok <- Validation.one_of(attrs, :redaction_class, @redaction_classes),
         :ok <- Validation.datetime(attrs, :occurred_at) do
      {:ok, struct!(__MODULE__, Validation.take(attrs, __struct__()))}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs), do: Validation.new!(__MODULE__, attrs, &new/1)
end

defmodule Mezzanine.AgentTurnEngine.AgentRunCursor do
  @moduledoc """
  Last-seen sequence catch-up contract for reconnecting clients and replay consumers.
  """

  alias Mezzanine.AgentTurnEngine.Validation

  @visibilities [:product, :operator, :internal]

  @enforce_keys [
    :cursor_ref,
    :ledger_ref,
    :tenant_ref,
    :actor_ref,
    :last_seq_seen,
    :visibility,
    :issued_at,
    :expires_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_lower_leakage(attrs),
         :ok <- Validation.ref(attrs, :cursor_ref, "agent-cursor://"),
         :ok <- Validation.ref(attrs, :ledger_ref, "agent-ledger://"),
         :ok <- Validation.ref(attrs, :tenant_ref, "tenant://"),
         :ok <- Validation.ref(attrs, :actor_ref, "actor://"),
         :ok <- Validation.non_negative_integer(attrs, :last_seq_seen),
         :ok <- Validation.one_of(attrs, :visibility, @visibilities),
         :ok <- Validation.datetime(attrs, :issued_at),
         :ok <- Validation.datetime(attrs, :expires_at) do
      {:ok, struct!(__MODULE__, Validation.take(attrs, __struct__()))}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs), do: Validation.new!(__MODULE__, attrs, &new/1)
end

defmodule Mezzanine.AgentTurnEngine.ExecutionReplay do
  @moduledoc """
  Deterministic replay, catch-up, and duplicate-work prevention contract.
  """

  alias Mezzanine.AgentTurnEngine.Validation

  @replay_kinds [:catchup, :resume_pending, :reconstruct_projection, :retry_lower_effect]
  @statuses [:planned, :running, :completed, :failed]

  @enforce_keys [
    :replay_ref,
    :ledger_ref,
    :replay_kind,
    :from_seq,
    :to_seq,
    :lower_reexecution_allowed?,
    :idempotency_key,
    :authority_ref,
    :evidence_refs,
    :status,
    :created_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_lower_leakage(attrs),
         :ok <- Validation.ref(attrs, :replay_ref, "agent-replay://"),
         :ok <- Validation.ref(attrs, :ledger_ref, "agent-ledger://"),
         :ok <- Validation.one_of(attrs, :replay_kind, @replay_kinds),
         :ok <- Validation.non_negative_integer(attrs, :from_seq),
         :ok <- Validation.non_negative_integer(attrs, :to_seq),
         :ok <- lower_reexecution_allowed(attrs),
         :ok <- Validation.required_binary(attrs, :idempotency_key),
         :ok <- Validation.ref(attrs, :authority_ref, "authority://"),
         :ok <- Validation.ref_list(attrs, :evidence_refs, ["evidence://", "receipt://"]),
         :ok <- Validation.one_of(attrs, :status, @statuses),
         :ok <- Validation.datetime(attrs, :created_at) do
      {:ok, struct!(__MODULE__, Validation.take(attrs, __struct__()))}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs), do: Validation.new!(__MODULE__, attrs, &new/1)

  defp lower_reexecution_allowed(attrs) do
    case {Validation.fetch(attrs, :replay_kind),
          Validation.fetch(attrs, :lower_reexecution_allowed?)} do
      {:retry_lower_effect, true} -> :ok
      {kind, false} when kind in [:catchup, :resume_pending, :reconstruct_projection] -> :ok
      {_kind, true} -> {:error, {:invalid, :lower_reexecution_allowed?, :retry_policy_required}}
      {_kind, other} when is_boolean(other) -> :ok
      {_kind, _other} -> {:error, {:invalid, :lower_reexecution_allowed?, :boolean}}
    end
  end
end

defmodule Mezzanine.AgentTurnEngine.AgentPendingInteraction do
  @moduledoc """
  Human/operator pause and resumable decision contract.
  """

  alias Mezzanine.AgentTurnEngine.Validation

  @kinds [
    :approval_required,
    :denial_confirmation,
    :credential_required,
    :budget_override_required,
    :tool_permission_required,
    :policy_exception_requested,
    :clarification_required
  ]
  @statuses [:open, :approved, :denied, :expired, :cancelled]

  @enforce_keys [
    :pending_ref,
    :ledger_ref,
    :decision_ref,
    :tenant_ref,
    :actor_ref,
    :kind,
    :prompt_summary,
    :requested_action_ref,
    :authority_ref,
    :opened_seq,
    :status,
    :expires_at
  ]
  defstruct @enforce_keys ++ [:resolved_at]

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_lower_leakage(attrs),
         :ok <- Validation.ref(attrs, :pending_ref, "agent-pending://"),
         :ok <- Validation.ref(attrs, :ledger_ref, "agent-ledger://"),
         :ok <- Validation.ref(attrs, :decision_ref, "decision://"),
         :ok <- Validation.ref(attrs, :tenant_ref, "tenant://"),
         :ok <- Validation.ref(attrs, :actor_ref, "actor://"),
         :ok <- Validation.one_of(attrs, :kind, @kinds),
         :ok <- Validation.required_binary(attrs, :prompt_summary),
         :ok <- Validation.ref(attrs, :requested_action_ref, "action://"),
         :ok <- Validation.ref(attrs, :authority_ref, "authority://"),
         :ok <- Validation.positive_integer(attrs, :opened_seq),
         :ok <- Validation.one_of(attrs, :status, @statuses),
         :ok <- Validation.datetime(attrs, :expires_at),
         :ok <- Validation.optional_datetime(attrs, :resolved_at) do
      {:ok, struct!(__MODULE__, Validation.take(attrs, __struct__()))}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs), do: Validation.new!(__MODULE__, attrs, &new/1)
end
