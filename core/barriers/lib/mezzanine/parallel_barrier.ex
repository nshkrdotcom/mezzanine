defmodule Mezzanine.ParallelBarrier do
  @moduledoc """
  Durable fan-out and fan-in barrier row with exact-close semantics.
  """

  use Ecto.Schema

  import Ecto.Changeset

  alias Ecto.Adapters.SQL
  alias Ecto.Multi
  alias Mezzanine.Execution.Repo
  alias Mezzanine.ParallelBarrierCompletion
  alias Mezzanine.Telemetry

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  @timestamps_opts [type: :utc_datetime_usec]
  @dialyzer [
    {:nowarn_function, completion_multi: 3},
    {:nowarn_function, close_ready_multi: 2}
  ]

  @type t :: %__MODULE__{
          id: Ecto.UUID.t() | nil,
          subject_id: Ecto.UUID.t() | nil,
          barrier_key: String.t() | nil,
          join_step_ref: String.t() | nil,
          expected_children: pos_integer() | nil,
          completed_children: non_neg_integer(),
          status: :open | :ready | :closed,
          trace_id: String.t() | nil,
          inserted_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil
        }

  @status_values [:open, :ready, :closed]

  @advance_barrier_sql """
  UPDATE parallel_barriers
  SET completed_children = completed_children + 1,
      status = CASE
        WHEN completed_children + 1 = expected_children THEN 'ready'
        ELSE status
      END,
      updated_at = $2
  WHERE id = $1::uuid
    AND status = 'open'
  RETURNING subject_id,
            join_step_ref,
            trace_id,
            completed_children,
            expected_children,
            status,
            (completed_children = expected_children AND status = 'ready') AS closed_by_me
  """

  @close_ready_sql """
  UPDATE parallel_barriers
  SET status = 'closed',
      updated_at = $2
  WHERE id = $1::uuid
    AND status = 'ready'
  RETURNING subject_id,
            join_step_ref,
            trace_id,
            completed_children,
            expected_children,
            status
  """

  @typedoc """
  Barrier-close progress returned after a child completion is recorded.
  """
  @type progress :: %{
          barrier_id: Ecto.UUID.t(),
          subject_id: Ecto.UUID.t(),
          join_step_ref: String.t(),
          trace_id: String.t(),
          completed_children: non_neg_integer(),
          expected_children: pos_integer(),
          status: :open | :ready | :closed,
          duplicate?: boolean(),
          closed_by_me: boolean()
        }

  schema "parallel_barriers" do
    field(:subject_id, :binary_id)
    field(:barrier_key, :string)
    field(:join_step_ref, :string)
    field(:expected_children, :integer)
    field(:completed_children, :integer, default: 0)
    field(:status, Ecto.Enum, values: @status_values, default: :open)
    field(:trace_id, :string)

    timestamps()
  end

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(barrier, attrs) do
    barrier
    |> cast(attrs, [
      :subject_id,
      :barrier_key,
      :join_step_ref,
      :expected_children,
      :completed_children,
      :status,
      :trace_id
    ])
    |> validate_required([
      :subject_id,
      :barrier_key,
      :join_step_ref,
      :expected_children,
      :trace_id
    ])
    |> validate_number(:expected_children, greater_than: 0)
    |> validate_number(:completed_children, greater_than_or_equal_to: 0)
    |> unique_constraint([:subject_id, :barrier_key],
      name: :parallel_barriers_subject_barrier_key_index
    )
  end

  @spec open(map()) :: {:ok, t()} | {:error, term()}
  def open(attrs) when is_map(attrs) do
    attrs = Map.new(attrs)

    %__MODULE__{}
    |> changeset(attrs)
    |> Repo.insert()
    |> case do
      {:ok, %__MODULE__{} = barrier} ->
        emit_barrier_open(barrier)
        {:ok, barrier}

      {:error, %Ecto.Changeset{} = changeset} ->
        with true <- duplicate_barrier_key?(changeset),
             %__MODULE__{} = existing <-
               Repo.get_by(__MODULE__,
                 subject_id: fetch_key!(attrs, :subject_id),
                 barrier_key: fetch_key!(attrs, :barrier_key)
               ),
             true <- compatible_open?(existing, attrs) do
          {:ok, existing}
        else
          _other -> {:error, changeset}
        end
    end
  end

  @spec fetch(Ecto.UUID.t()) :: {:ok, t()} | :error
  def fetch(barrier_id) when is_binary(barrier_id) do
    case Repo.get(__MODULE__, barrier_id) do
      %__MODULE__{} = barrier -> {:ok, barrier}
      nil -> :error
    end
  end

  @spec completion_multi(Ecto.UUID.t(), Ecto.UUID.t(), keyword()) :: Ecto.Multi.t()
  def completion_multi(barrier_id, child_execution_id, opts \\ [])
      when is_binary(barrier_id) and is_binary(child_execution_id) do
    now = Keyword.get(opts, :now, DateTime.utc_now()) |> DateTime.truncate(:microsecond)

    Multi.new()
    |> Multi.run(:parallel_barrier_completion, fn repo, _changes ->
      insert_completion(repo, barrier_id, child_execution_id, now)
    end)
    |> Multi.run(:parallel_barrier_progress, fn repo, %{parallel_barrier_completion: result} ->
      case result do
        :duplicate -> duplicate_progress(repo, barrier_id)
        :inserted -> advance_barrier(repo, barrier_id, now)
      end
    end)
  end

  @spec record_child_completion(Ecto.UUID.t(), Ecto.UUID.t(), keyword()) ::
          {:ok, progress()} | {:error, term()}
  def record_child_completion(barrier_id, child_execution_id, opts \\ []) do
    started_at = System.monotonic_time()

    completion_multi(barrier_id, child_execution_id, opts)
    |> Repo.transaction()
    |> case do
      {:ok, %{parallel_barrier_progress: progress}} ->
        emit_child_completion(progress, child_execution_id, started_at)
        {:ok, progress}

      {:error, _step, error, _changes} ->
        {:error, error}
    end
  end

  @spec close_ready_multi(Ecto.UUID.t(), keyword()) :: Ecto.Multi.t()
  def close_ready_multi(barrier_id, opts \\ []) when is_binary(barrier_id) do
    now = Keyword.get(opts, :now, DateTime.utc_now()) |> DateTime.truncate(:microsecond)

    Multi.new()
    |> Multi.run(:parallel_barrier_close, fn repo, _changes ->
      close_ready(repo, barrier_id, now)
    end)
  end

  defp insert_completion(repo, barrier_id, child_execution_id, now) do
    attrs = %{
      barrier_id: barrier_id,
      child_execution_id: child_execution_id,
      inserted_at: now,
      updated_at: now
    }

    case repo.insert_all(
           ParallelBarrierCompletion,
           [attrs],
           on_conflict: :nothing,
           conflict_target: [:barrier_id, :child_execution_id]
         ) do
      {0, _rows} -> {:ok, :duplicate}
      {1, _rows} -> {:ok, :inserted}
      other -> {:error, {:unexpected_completion_insert_result, other}}
    end
  end

  defp duplicate_progress(repo, barrier_id) do
    case repo.get(__MODULE__, barrier_id) do
      %__MODULE__{} = barrier ->
        {:ok, barrier_progress(barrier, duplicate?: true, closed_by_me: false)}

      nil ->
        {:error, {:barrier_not_found, barrier_id}}
    end
  end

  defp advance_barrier(repo, barrier_id, now) do
    case SQL.query(repo, @advance_barrier_sql, [dump_uuid!(barrier_id), now]) do
      {:ok,
       %{
         rows: [
           [
             subject_id,
             join_step_ref,
             trace_id,
             completed_children,
             expected_children,
             status,
             closed_by_me
           ]
         ]
       }} ->
        {:ok,
         %{
           barrier_id: barrier_id,
           subject_id: load_uuid!(subject_id),
           join_step_ref: join_step_ref,
           trace_id: trace_id,
           completed_children: completed_children,
           expected_children: expected_children,
           status: normalize_status(status),
           duplicate?: false,
           closed_by_me: closed_by_me
         }}

      {:ok, %{rows: []}} ->
        barrier_not_open_error(repo, barrier_id)

      {:error, error} ->
        {:error, error}
    end
  end

  defp close_ready(repo, barrier_id, now) do
    case SQL.query(repo, @close_ready_sql, [dump_uuid!(barrier_id), now]) do
      {:ok,
       %{
         rows: [
           [subject_id, join_step_ref, trace_id, completed_children, expected_children, status]
         ]
       }} ->
        {:ok,
         %{
           barrier_id: barrier_id,
           subject_id: load_uuid!(subject_id),
           join_step_ref: join_step_ref,
           trace_id: trace_id,
           completed_children: completed_children,
           expected_children: expected_children,
           status: normalize_status(status),
           duplicate?: false,
           closed_by_me: false
         }}

      {:ok, %{rows: []}} ->
        case repo.get(__MODULE__, barrier_id) do
          %__MODULE__{status: :closed} = barrier ->
            {:ok, barrier_progress(barrier, duplicate?: true, closed_by_me: false)}

          %__MODULE__{status: status} ->
            {:error, {:barrier_not_ready, barrier_id, status}}

          nil ->
            {:error, {:barrier_not_found, barrier_id}}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  defp barrier_not_open_error(repo, barrier_id) do
    case repo.get(__MODULE__, barrier_id) do
      %__MODULE__{status: status} ->
        {:error, {:barrier_not_open, barrier_id, status}}

      nil ->
        {:error, {:barrier_not_found, barrier_id}}
    end
  end

  defp barrier_progress(%__MODULE__{} = barrier, opts) do
    %{
      barrier_id: barrier.id,
      subject_id: barrier.subject_id,
      join_step_ref: barrier.join_step_ref,
      trace_id: barrier.trace_id,
      completed_children: barrier.completed_children,
      expected_children: barrier.expected_children,
      status: barrier.status,
      duplicate?: Keyword.get(opts, :duplicate?, false),
      closed_by_me: Keyword.get(opts, :closed_by_me, false)
    }
  end

  defp emit_barrier_open(%__MODULE__{} = barrier) do
    Telemetry.emit(
      [:barrier, :open],
      %{count: 1},
      barrier_metadata(barrier, %{
        barrier_id: barrier.id,
        barrier_key: barrier.barrier_key,
        join_step_ref: barrier.join_step_ref,
        expected_children: barrier.expected_children,
        completed_children: barrier.completed_children,
        barrier_status: barrier.status
      })
    )
  end

  defp emit_child_completion(progress, child_execution_id, started_at) do
    Telemetry.emit(
      [:barrier, :child_completion],
      %{count: 1, latency_ms: Telemetry.monotonic_duration_ms(started_at)},
      barrier_metadata(progress, %{
        barrier_id: progress.barrier_id,
        execution_id: child_execution_id,
        join_step_ref: progress.join_step_ref,
        expected_children: progress.expected_children,
        completed_children: progress.completed_children,
        barrier_status: progress.status,
        duplicate: progress.duplicate?,
        closed_by_me: progress.closed_by_me
      })
    )
  end

  defp barrier_metadata(%__MODULE__{} = barrier, extra) do
    Map.merge(
      %{
        subject_id: barrier.subject_id,
        trace_id: barrier.trace_id
      },
      extra
    )
  end

  defp barrier_metadata(progress, extra) when is_map(progress) do
    Map.merge(
      %{
        subject_id: progress.subject_id,
        trace_id: progress.trace_id
      },
      extra
    )
  end

  defp compatible_open?(existing, attrs) do
    existing.join_step_ref == fetch_key!(attrs, :join_step_ref) and
      existing.expected_children == fetch_key!(attrs, :expected_children) and
      existing.trace_id == fetch_key!(attrs, :trace_id)
  end

  defp duplicate_barrier_key?(%Ecto.Changeset{errors: errors}) do
    Enum.any?(errors, fn
      {_field, {_message, metadata}} ->
        metadata[:constraint] in [:unique, :unsafe_unique] and
          metadata[:constraint_name] == "parallel_barriers_subject_barrier_key_index"

      _other ->
        false
    end)
  end

  defp normalize_status(value) when is_atom(value), do: value
  defp normalize_status(value) when is_binary(value), do: String.to_existing_atom(value)

  defp fetch_key!(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.fetch!(attrs, Atom.to_string(key))
    end
  end

  defp dump_uuid!(uuid), do: Ecto.UUID.dump!(uuid)
  defp load_uuid!(uuid), do: Ecto.UUID.load!(uuid)
end
