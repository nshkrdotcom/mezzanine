defmodule Mezzanine.Execution.LifecycleContinuation do
  @moduledoc """
  Durable post-commit lifecycle continuation record and recovery policy.

  Continuations make lifecycle follow-up work operator-visible instead of
  leaving recursive post-commit advancement stranded in worker error state.
  """

  use Ecto.Schema

  import Ecto.Changeset
  import Ecto.Query

  alias Mezzanine.Execution.CompensationEvidence
  alias Mezzanine.Execution.Repo

  @primary_key {:continuation_id, :string, autogenerate: false}
  @derive {Jason.Encoder,
           only: [
             :continuation_id,
             :tenant_id,
             :installation_id,
             :subject_id,
             :execution_id,
             :from_state,
             :target_transition,
             :attempt_count,
             :next_attempt_at,
             :last_error_class,
             :last_error_message,
             :trace_id,
             :status,
             :actor_ref,
             :metadata
           ]}
  schema "lifecycle_continuations" do
    field(:tenant_id, :string)
    field(:installation_id, :string)
    field(:subject_id, Ecto.UUID)
    field(:execution_id, Ecto.UUID)
    field(:from_state, :string)
    field(:target_transition, :string)
    field(:attempt_count, :integer, default: 0)
    field(:next_attempt_at, :utc_datetime_usec)
    field(:last_error_class, :string)
    field(:last_error_message, :string)
    field(:trace_id, :string)

    field(:status, Ecto.Enum,
      values: [:pending, :running, :retry_scheduled, :dead_lettered, :completed],
      default: :pending
    )

    field(:actor_ref, :map, default: %{})
    field(:metadata, :map, default: %{})

    timestamps(type: :utc_datetime_usec)
  end

  @type status :: :pending | :running | :retry_scheduled | :dead_lettered | :completed
  @type t :: %__MODULE__{}

  @required_fields [
    :continuation_id,
    :tenant_id,
    :installation_id,
    :subject_id,
    :execution_id,
    :from_state,
    :target_transition,
    :next_attempt_at,
    :trace_id,
    :status
  ]

  @optional_fields [:attempt_count, :last_error_class, :last_error_message, :actor_ref, :metadata]
  @retryable_error_classes ["transient_lock", "dependency_unavailable", "worker_crash"]
  @terminal_error_classes ["invalid_transition", "retry_budget_exhausted", "operator_waived"]
  @default_max_attempts 3
  @default_backoff_ms 5_000
  @target_key "continuation_target"
  @target_kinds ["owner_command", "workflow_signal"]
  @target_required_fields %{
    "owner_command" => ["owner", "command", "idempotency_key"],
    "workflow_signal" => ["workflow_id", "signal", "idempotency_key"]
  }

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(%__MODULE__{} = continuation, attrs) when is_map(attrs) do
    continuation
    |> cast(Map.new(attrs), @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> validate_number(:attempt_count, greater_than_or_equal_to: 0)
    |> validate_last_error_class()
  end

  defp validate_last_error_class(changeset) do
    validate_change(changeset, :last_error_class, fn
      :last_error_class, nil ->
        []

      :last_error_class, value ->
        if value in (@retryable_error_classes ++ @terminal_error_classes) do
          []
        else
          [last_error_class: "is not a known lifecycle continuation error class"]
        end
    end)
  end

  @spec enqueue(map(), keyword()) :: {:ok, t()} | {:error, Ecto.Changeset.t()}
  def enqueue(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now() |> DateTime.truncate(:microsecond))

    attrs =
      attrs
      |> Map.new()
      |> Map.put_new(:continuation_id, Ecto.UUID.generate())
      |> Map.put_new(:status, :pending)
      |> Map.put_new(:attempt_count, 0)
      |> Map.put_new(:next_attempt_at, now)
      |> Map.put_new(:actor_ref, %{})
      |> Map.put_new(:metadata, %{})

    %__MODULE__{}
    |> changeset(attrs)
    |> Repo.insert(on_conflict: :nothing)
  end

  @spec fetch(String.t()) :: {:ok, t()} | {:error, :not_found}
  def fetch(continuation_id) when is_binary(continuation_id) do
    case Repo.get(__MODULE__, continuation_id) do
      %__MODULE__{} = continuation -> {:ok, continuation}
      nil -> {:error, :not_found}
    end
  end

  @spec list_operator(String.t(), String.t(), keyword()) :: {:ok, [t()]}
  def list_operator(tenant_id, installation_id, opts \\ [])
      when is_binary(tenant_id) and is_binary(installation_id) and is_list(opts) do
    statuses = Keyword.get(opts, :statuses, [:pending, :retry_scheduled, :dead_lettered])

    rows =
      __MODULE__
      |> where([c], c.tenant_id == ^tenant_id)
      |> where([c], c.installation_id == ^installation_id)
      |> where([c], c.status in ^statuses)
      |> order_by([c], asc: c.next_attempt_at, asc: c.continuation_id)
      |> Repo.all()

    {:ok, rows}
  end

  @spec list_for_subject(String.t(), Ecto.UUID.t(), keyword()) :: {:ok, [t()]}
  def list_for_subject(tenant_id, subject_id, opts \\ [])
      when is_binary(tenant_id) and is_binary(subject_id) and is_list(opts) do
    statuses =
      Keyword.get(opts, :statuses, [
        :pending,
        :running,
        :retry_scheduled,
        :dead_lettered
      ])

    rows =
      __MODULE__
      |> where([c], c.tenant_id == ^tenant_id)
      |> where([c], c.subject_id == ^subject_id)
      |> where([c], c.status in ^statuses)
      |> order_by([c], asc: c.next_attempt_at, asc: c.continuation_id)
      |> Repo.all()

    {:ok, rows}
  end

  @spec process(String.t(), keyword()) ::
          {:ok, t() | :already_completed | :already_running | :not_due}
          | {:error, term()}
  def process(continuation_id, opts \\ []) when is_binary(continuation_id) and is_list(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now() |> DateTime.truncate(:microsecond))

    with {:ok, dispatcher} <- fetch_dispatcher(opts) do
      case claim_for_processing(continuation_id, now) do
        {:ok, %__MODULE__{} = continuation} ->
          dispatch_continuation(dispatcher, continuation, now, opts)

        {:ok, terminal_status} ->
          {:ok, terminal_status}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @spec dispatch_target(t()) :: {:ok, map()} | {:error, term()}
  def dispatch_target(%__MODULE__{metadata: metadata}) do
    case map_value(metadata, @target_key) do
      %{} = target -> normalize_target(target)
      _missing -> {:error, :missing_lifecycle_continuation_target}
    end
  end

  @spec retry(String.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def retry(continuation_id, opts \\ []) when is_binary(continuation_id) and is_list(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now() |> DateTime.truncate(:microsecond))

    Repo.transaction(fn ->
      with {:ok, continuation} <- locked_fetch(continuation_id),
           :ok <- ensure_retryable_operator_state(continuation),
           {:ok, evidence} <- operator_action_evidence(continuation, :operator_retry, opts) do
        continuation
        |> changeset(%{
          status: :pending,
          next_attempt_at: now,
          last_error_class: nil,
          last_error_message: nil,
          metadata: append_evidence(continuation, evidence)
        })
        |> Repo.update!()
      else
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end

  @spec waive(String.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def waive(continuation_id, opts \\ []) when is_binary(continuation_id) and is_list(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now() |> DateTime.truncate(:microsecond))

    Repo.transaction(fn ->
      with {:ok, continuation} <- locked_fetch(continuation_id),
           {:ok, evidence} <- operator_action_evidence(continuation, :operator_waive, opts) do
        continuation
        |> changeset(%{
          status: :completed,
          next_attempt_at: now,
          last_error_class: "operator_waived",
          last_error_message: Keyword.get(opts, :reason, "operator waived continuation"),
          metadata: append_evidence(continuation, evidence)
        })
        |> Repo.update!()
      else
        {:error, reason} ->
          Repo.rollback(reason)
      end
    end)
  end

  defp operator_action_evidence(%__MODULE__{} = continuation, compensation_kind, opts) do
    continuation
    |> evidence_attrs(:operator_action, opts,
      compensation_kind: compensation_kind,
      failure_class: Atom.to_string(compensation_kind),
      failure_reason: Keyword.get(opts, :reason, "operator compensation action"),
      operator_action_ref: Keyword.get(opts, :operator_action_ref),
      operator_actor_ref: Keyword.get(opts, :operator_actor_ref),
      authority_decision_ref: Keyword.get(opts, :authority_decision_ref),
      safe_action: Keyword.get(opts, :safe_action),
      blast_radius: Keyword.get(opts, :blast_radius)
    )
    |> CompensationEvidence.record()
  end

  defp append_evidence(%__MODULE__{metadata: metadata}, evidence),
    do: CompensationEvidence.append_to_metadata(metadata, evidence)

  defp evidence_attrs(%__MODULE__{} = continuation, event_kind, opts, overrides) do
    target = evidence_target(continuation)
    max_attempts = Keyword.get(opts, :max_attempts, @default_max_attempts)
    backoff_ms = Keyword.get(opts, :backoff_ms, @default_backoff_ms)
    attempt_number = max(continuation.attempt_count, 1)
    metadata = continuation.metadata || %{}

    %{
      event_kind: event_kind,
      compensation_ref:
        metadata_value(metadata, "compensation_ref") ||
          "compensation:lifecycle-continuation:#{continuation.continuation_id}",
      source_context: "lifecycle_continuation",
      source_event_ref: continuation.continuation_id,
      failed_step_ref: continuation.target_transition,
      tenant_id: continuation.tenant_id,
      installation_id: continuation.installation_id,
      trace_id: continuation.trace_id,
      causation_id:
        metadata_value(metadata, "causation_id") ||
          "lifecycle-continuation:#{continuation.continuation_id}",
      canonical_idempotency_key:
        target_value(target, "idempotency_key") ||
          "lifecycle-continuation:#{continuation.continuation_id}",
      compensation_owner: compensation_owner(target),
      compensation_kind: Keyword.get(overrides, :compensation_kind, :retry),
      owner_command_or_signal: target,
      attempt_ref: "#{continuation.continuation_id}:#{event_kind}:#{attempt_number}",
      attempt_number: attempt_number,
      max_attempts: max_attempts,
      retry_policy: %{max_attempts: max_attempts, backoff_ms: backoff_ms},
      dead_letter_ref:
        metadata_value(metadata, "dead_letter_ref") ||
          "dead-letter:lifecycle-continuation:#{continuation.continuation_id}",
      failure_class: Keyword.get(overrides, :failure_class),
      failure_reason: Keyword.get(overrides, :failure_reason),
      next_attempt_at: Keyword.get(overrides, :next_attempt_at),
      operator_action_ref: Keyword.get(overrides, :operator_action_ref),
      operator_actor_ref: Keyword.get(overrides, :operator_actor_ref),
      authority_decision_ref: Keyword.get(overrides, :authority_decision_ref),
      safe_action: Keyword.get(overrides, :safe_action),
      blast_radius: Keyword.get(overrides, :blast_radius),
      audit_or_evidence_ref:
        Keyword.get(opts, :audit_or_evidence_ref) ||
          metadata_value(metadata, "audit_or_evidence_ref") ||
          "audit:lifecycle-continuation:#{continuation.continuation_id}:#{event_kind}:#{attempt_number}",
      release_manifest_ref: CompensationEvidence.release_manifest_ref()
    }
  end

  defp evidence_target(%__MODULE__{} = continuation) do
    case dispatch_target(continuation) do
      {:ok, target} -> target
      {:error, reason} -> %{"kind" => "invalid_target", "reason" => inspect(reason)}
    end
  end

  defp compensation_owner(%{"kind" => "workflow_signal"}), do: "workflow_lifecycle"
  defp compensation_owner(%{"owner" => owner}) when is_binary(owner), do: owner
  defp compensation_owner(_target), do: "lifecycle_continuation"

  defp target_value(target, field),
    do: Map.get(target, field) || Map.get(target, String.to_atom(field))

  defp metadata_value(metadata, field),
    do: Map.get(metadata, field) || Map.get(metadata, String.to_atom(field))

  defp claim_for_processing(continuation_id, now) do
    Repo.transaction(fn ->
      with {:ok, continuation} <- locked_fetch(continuation_id),
           :ok <- ensure_due_for_processing(continuation, now) do
        continuation
        |> changeset(%{
          status: :running,
          attempt_count: continuation.attempt_count + 1,
          next_attempt_at: now
        })
        |> Repo.update!()
      else
        {:ok, terminal} -> terminal
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end

  defp locked_fetch(continuation_id) do
    __MODULE__
    |> where([c], c.continuation_id == ^continuation_id)
    |> lock("FOR UPDATE")
    |> Repo.one()
    |> case do
      %__MODULE__{} = continuation -> {:ok, continuation}
      nil -> {:error, :not_found}
    end
  end

  defp ensure_due_for_processing(%__MODULE__{status: :completed}, _now),
    do: {:ok, :already_completed}

  defp ensure_due_for_processing(%__MODULE__{status: :running}, _now),
    do: {:ok, :already_running}

  defp ensure_due_for_processing(%__MODULE__{status: :dead_lettered}, _now),
    do: {:ok, :already_completed}

  defp ensure_due_for_processing(%__MODULE__{next_attempt_at: next_attempt_at}, now) do
    if DateTime.compare(next_attempt_at, now) in [:lt, :eq] do
      :ok
    else
      {:ok, :not_due}
    end
  end

  defp ensure_retryable_operator_state(%__MODULE__{status: status})
       when status in [:retry_scheduled, :dead_lettered],
       do: :ok

  defp ensure_retryable_operator_state(%__MODULE__{status: status}),
    do: {:error, {:continuation_not_retryable, status}}

  defp fetch_dispatcher(opts) do
    case Keyword.get(opts, :dispatcher) do
      nil ->
        {:error, :missing_lifecycle_continuation_dispatcher}

      dispatcher when is_atom(dispatcher) ->
        if function_exported?(dispatcher, :dispatch_lifecycle_continuation, 2) do
          {:ok, dispatcher}
        else
          {:error, {:invalid_lifecycle_continuation_dispatcher, dispatcher}}
        end

      _missing ->
        {:error, :missing_lifecycle_continuation_dispatcher}
    end
  end

  defp dispatch_continuation(dispatcher, %__MODULE__{} = continuation, now, opts) do
    case dispatch_target(continuation) do
      {:ok, target} ->
        case dispatcher.dispatch_lifecycle_continuation(continuation, target) do
          :ok -> mark_completed(continuation, now)
          {:ok, _result} -> mark_completed(continuation, now)
          {:error, reason} -> record_failure(continuation, reason, now, opts)
        end

      {:error, reason} ->
        record_failure(continuation, {:invalid_transition, reason}, now, opts)
    end
  end

  defp normalize_target(target) when is_map(target) do
    normalized = Map.new(target, fn {key, value} -> {to_string(key), value} end)

    with {:ok, kind} <- normalize_target_kind(Map.get(normalized, "kind")),
         :ok <- ensure_target_fields(kind, normalized) do
      {:ok, Map.put(normalized, "kind", kind)}
    end
  end

  defp normalize_target_kind(kind) when is_atom(kind),
    do: normalize_target_kind(Atom.to_string(kind))

  defp normalize_target_kind(kind) when kind in @target_kinds, do: {:ok, kind}

  defp normalize_target_kind(kind),
    do: {:error, {:invalid_lifecycle_continuation_target_kind, kind}}

  defp ensure_target_fields(kind, target) do
    missing =
      @target_required_fields
      |> Map.fetch!(kind)
      |> Enum.reject(&present_binary?(Map.get(target, &1)))

    case missing do
      [] -> :ok
      _ -> {:error, {:missing_lifecycle_continuation_target_fields, kind, missing}}
    end
  end

  defp present_binary?(value), do: is_binary(value) and byte_size(value) > 0

  defp map_value(map, @target_key) when is_map(map),
    do: Map.get(map, @target_key) || Map.get(map, :continuation_target)

  defp map_value(_map, _key), do: nil

  defp mark_completed(%__MODULE__{} = continuation, now) do
    continuation
    |> changeset(%{
      status: :completed,
      next_attempt_at: now,
      last_error_class: nil,
      last_error_message: nil
    })
    |> Repo.update()
  end

  defp record_failure(%__MODULE__{} = continuation, reason, now, opts) do
    class = classify_error(reason)
    max_attempts = Keyword.get(opts, :max_attempts, @default_max_attempts)

    if terminal_failure?(class, continuation.attempt_count, max_attempts) do
      dead_letter(
        continuation,
        terminal_class(class, continuation.attempt_count, max_attempts),
        reason,
        now,
        opts
      )
    else
      schedule_retry(continuation, class, reason, now, opts)
    end
  end

  defp classify_error(:lock_timeout), do: "transient_lock"
  defp classify_error(:crash), do: "worker_crash"
  defp classify_error(:dependency_unavailable), do: "dependency_unavailable"
  defp classify_error(:invalid_transition), do: "invalid_transition"
  defp classify_error({:invalid_transition, _detail}), do: "invalid_transition"
  defp classify_error({:lock_timeout, _detail}), do: "transient_lock"
  defp classify_error({:dependency_unavailable, _detail}), do: "dependency_unavailable"
  defp classify_error(_reason), do: "worker_crash"

  defp terminal_failure?("invalid_transition", _attempt_count, _max_attempts), do: true
  defp terminal_failure?(_class, attempt_count, max_attempts), do: attempt_count >= max_attempts

  defp terminal_class("invalid_transition", _attempt_count, _max_attempts),
    do: "invalid_transition"

  defp terminal_class(_class, _attempt_count, _max_attempts), do: "retry_budget_exhausted"

  defp schedule_retry(%__MODULE__{} = continuation, class, reason, now, opts) do
    backoff_ms = Keyword.get(opts, :backoff_ms, @default_backoff_ms)
    next_attempt_at = DateTime.add(now, backoff_ms * continuation.attempt_count, :millisecond)

    with {:ok, evidence} <-
           continuation
           |> evidence_attrs(:retry_scheduled, opts,
             failure_class: class,
             failure_reason: inspect(reason),
             next_attempt_at: next_attempt_at
           )
           |> CompensationEvidence.record() do
      continuation
      |> changeset(%{
        status: :retry_scheduled,
        next_attempt_at: next_attempt_at,
        last_error_class: class,
        last_error_message: inspect(reason),
        metadata: append_evidence(continuation, evidence)
      })
      |> Repo.update()
    end
  end

  defp dead_letter(%__MODULE__{} = continuation, class, reason, now, opts) do
    with {:ok, evidence} <-
           continuation
           |> evidence_attrs(:dead_lettered, opts,
             failure_class: class,
             failure_reason: inspect(reason)
           )
           |> CompensationEvidence.record() do
      continuation
      |> changeset(%{
        status: :dead_lettered,
        next_attempt_at: now,
        last_error_class: class,
        last_error_message: inspect(reason),
        metadata: append_evidence(continuation, evidence)
      })
      |> Repo.update()
    end
  end
end
