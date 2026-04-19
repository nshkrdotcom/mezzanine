defmodule Mezzanine.Execution.LifecycleContinuation do
  @moduledoc """
  Durable post-commit lifecycle continuation record and recovery policy.

  Continuations make lifecycle follow-up work operator-visible instead of
  leaving recursive post-commit advancement stranded in worker error state.
  """

  use Ecto.Schema

  import Ecto.Changeset
  import Ecto.Query

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
    handler = Keyword.fetch!(opts, :handler)

    case claim_for_processing(continuation_id, now) do
      {:ok, %__MODULE__{} = continuation} ->
        case handler.(continuation) do
          :ok -> mark_completed(continuation, now)
          {:ok, _result} -> mark_completed(continuation, now)
          {:error, reason} -> record_failure(continuation, reason, now, opts)
        end

      {:ok, terminal_status} ->
        {:ok, terminal_status}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec retry(String.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def retry(continuation_id, opts \\ []) when is_binary(continuation_id) and is_list(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now() |> DateTime.truncate(:microsecond))

    Repo.transaction(fn ->
      with {:ok, continuation} <- locked_fetch(continuation_id),
           :ok <- ensure_retryable_operator_state(continuation) do
        continuation
        |> changeset(%{
          status: :pending,
          next_attempt_at: now,
          last_error_class: nil,
          last_error_message: nil
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
      case locked_fetch(continuation_id) do
        {:ok, continuation} ->
          continuation
          |> changeset(%{
            status: :completed,
            next_attempt_at: now,
            last_error_class: "operator_waived",
            last_error_message: Keyword.get(opts, :reason, "operator waived continuation")
          })
          |> Repo.update!()

        {:error, reason} ->
          Repo.rollback(reason)
      end
    end)
  end

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
        now
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

    continuation
    |> changeset(%{
      status: :retry_scheduled,
      next_attempt_at: next_attempt_at,
      last_error_class: class,
      last_error_message: inspect(reason)
    })
    |> Repo.update()
  end

  defp dead_letter(%__MODULE__{} = continuation, class, reason, now) do
    continuation
    |> changeset(%{
      status: :dead_lettered,
      next_attempt_at: now,
      last_error_class: class,
      last_error_message: inspect(reason)
    })
    |> Repo.update()
  end
end
