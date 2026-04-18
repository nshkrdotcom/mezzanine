defmodule Mezzanine.JobOutbox do
  @moduledoc """
  Small orchestration seam for durable job ownership on the active runtime path.
  """

  @type job_ref :: %{
          provider: :oban,
          job_id: pos_integer(),
          queue: String.t()
        }

  @callback enqueue(atom() | String.t(), module(), map(), keyword()) ::
              {:ok, job_ref()} | {:error, term()}
  @callback cancel(job_ref() | pos_integer()) :: :ok | {:error, term()}
  @callback reschedule(job_ref() | pos_integer(), DateTime.t()) :: :ok | {:error, term()}
  @callback unique_declaration(module()) :: keyword() | nil
  @callback snooze_response(non_neg_integer()) :: tuple()

  @spec enqueue(atom() | String.t(), module(), map(), keyword()) ::
          {:ok, job_ref()} | {:error, term()}
  def enqueue(queue, worker_module, args, opts \\ []) do
    implementation().enqueue(queue, worker_module, args, opts)
  end

  @spec cancel(job_ref() | pos_integer()) :: :ok | {:error, term()}
  def cancel(job_ref), do: implementation().cancel(job_ref)

  @spec reschedule(job_ref() | pos_integer(), DateTime.t()) :: :ok | {:error, term()}
  def reschedule(job_ref, scheduled_at), do: implementation().reschedule(job_ref, scheduled_at)

  @spec unique_declaration(module()) :: keyword() | nil
  def unique_declaration(worker_module), do: implementation().unique_declaration(worker_module)

  @spec snooze_response(non_neg_integer()) :: tuple()
  def snooze_response(cooldown_ms), do: implementation().snooze_response(cooldown_ms)

  defp implementation do
    Application.get_env(
      :mezzanine_execution_engine,
      :job_outbox_impl,
      Mezzanine.JobOutbox.Oban
    )
  end
end

defmodule Mezzanine.JobOutbox.Oban do
  @moduledoc false

  @behaviour Mezzanine.JobOutbox

  alias Ecto.Adapters.SQL
  import Ecto.Query

  alias Mezzanine.Execution.Repo

  @oban_name Mezzanine.Execution.Oban

  @impl true
  def enqueue(queue, worker_module, args, opts \\ []) do
    args = stringify_keys(Map.new(args))
    job_opts = normalize_job_opts(queue, worker_module, opts)

    case unique_declaration(worker_module) do
      nil ->
        insert_job(worker_module, args, job_opts)

      unique_opts ->
        enqueue_unique(queue, worker_module, args, job_opts, unique_opts)
    end
  end

  @impl true
  def cancel(job_ref) do
    now = DateTime.utc_now()
    job_id = job_id(job_ref)

    case Repo.update_all(
           from(job in Oban.Job,
             where:
               job.id == ^job_id and
                 job.state in ["available", "scheduled", "retryable", "executing"]
           ),
           set: [state: "cancelled", cancelled_at: now]
         ) do
      {1, _rows} -> :ok
      _other -> {:error, :job_not_cancellable}
    end
  end

  @impl true
  def reschedule(job_ref, scheduled_at) do
    job_id = job_id(job_ref)

    case Repo.update_all(
           from(job in Oban.Job,
             where:
               job.id == ^job_id and
                 job.state in ["available", "scheduled", "retryable"]
           ),
           set: [state: "scheduled", scheduled_at: scheduled_at]
         ) do
      {1, _rows} -> :ok
      _other -> {:error, :job_not_reschedulable}
    end
  end

  @impl true
  def unique_declaration(worker_module) do
    if function_exported?(worker_module, :unique_declaration, 0) do
      worker_module.unique_declaration()
    end
  end

  @impl true
  def snooze_response(cooldown_ms) do
    {:snooze, max(1, div(cooldown_ms + 999, 1000))}
  end

  defp normalize_job_opts(queue, worker_module, opts) do
    base =
      opts
      |> Keyword.take([:max_attempts, :priority, :meta, :replace])
      |> Keyword.put(:queue, to_string(queue))

    base =
      case Keyword.get(opts, :scheduled_at) do
        %DateTime{} = scheduled_at -> Keyword.put(base, :scheduled_at, scheduled_at)
        _other -> base
      end

    case unique_declaration(worker_module) do
      nil -> base
      unique -> Keyword.put(base, :unique, unique)
    end
  end

  defp job_id(%{job_id: job_id}) when is_integer(job_id), do: job_id
  defp job_id(job_id) when is_integer(job_id), do: job_id

  defp enqueue_unique(queue, worker_module, args, job_opts, unique_opts) do
    Repo.transaction(fn ->
      :ok = lock_unique_scope(queue, worker_module, args, unique_opts)
      existing_or_insert_unique_job(queue, worker_module, args, job_opts, unique_opts)
    end)
    |> case do
      {:ok, %Oban.Job{} = job} ->
        {:ok, %{provider: :oban, job_id: job.id, queue: job.queue}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp existing_or_insert_unique_job(queue, worker_module, args, job_opts, unique_opts) do
    case find_existing_unique_job(queue, worker_module, args, unique_opts) do
      %Oban.Job{} = existing_job ->
        existing_job

      nil ->
        insert_unique_job(worker_module, args, job_opts)
    end
  end

  defp insert_unique_job(worker_module, args, job_opts) do
    case insert_job(worker_module, args, job_opts) do
      {:ok, %{job_id: job_id}} -> Repo.get!(Oban.Job, job_id)
      {:error, error} -> Repo.rollback(error)
    end
  end

  defp insert_job(worker_module, args, job_opts) do
    case Oban.insert(@oban_name, worker_module.new(args, job_opts)) do
      {:ok, %Oban.Job{} = job} ->
        {:ok, %{provider: :oban, job_id: job.id, queue: job.queue}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp lock_unique_scope(queue, worker_module, args, unique_opts) do
    lock_values = unique_lock_values(queue, worker_module, args, unique_opts)

    SQL.query!(
      Repo,
      "SELECT pg_advisory_xact_lock($1, $2)",
      [
        :erlang.phash2(lock_values.worker, 2_147_483_647),
        :erlang.phash2({lock_values.queue, lock_values.args}, 2_147_483_647)
      ]
    )

    :ok
  end

  defp find_existing_unique_job(queue, worker_module, args, unique_opts) do
    worker = Oban.Worker.to_string(worker_module)
    queue = to_string(queue)
    states = Enum.map(unique_states(unique_opts), &to_string/1)

    conditions =
      Enum.reduce(
        unique_keys(unique_opts),
        dynamic([job], job.worker == ^worker and job.queue == ^queue and job.state in ^states),
        fn key, dynamic ->
          key = to_string(key)

          case Map.fetch(args, key) do
            {:ok, nil} ->
              dynamic([job], ^dynamic and fragment("? ->> ? IS NULL", job.args, ^key))

            {:ok, value} ->
              dynamic(
                [job],
                ^dynamic and fragment("? ->> ? = ?", job.args, ^key, ^to_string(value))
              )

            :error ->
              dynamic([job], ^dynamic and fragment("? ->> ? IS NULL", job.args, ^key))
          end
        end
      )
      |> maybe_apply_unique_period(unique_opts)

    from(job in Oban.Job,
      where: ^conditions,
      order_by: [asc: job.id],
      limit: 1
    )
    |> Repo.one()
  end

  defp maybe_apply_unique_period(dynamic, unique_opts) do
    case Keyword.get(unique_opts, :period, :infinity) do
      :infinity ->
        dynamic

      seconds when is_integer(seconds) and seconds > 0 ->
        cutoff = DateTime.add(DateTime.utc_now(), -seconds, :second)
        dynamic([job], ^dynamic and job.inserted_at >= ^cutoff)

      _other ->
        dynamic
    end
  end

  defp unique_lock_values(queue, worker_module, args, unique_opts) do
    %{
      worker: Oban.Worker.to_string(worker_module),
      queue: to_string(queue),
      args:
        unique_opts
        |> unique_keys()
        |> Enum.sort()
        |> Enum.map(fn key -> {to_string(key), Map.get(args, to_string(key))} end)
    }
  end

  defp unique_keys(unique_opts), do: Keyword.get(unique_opts, :keys, [])
  defp unique_states(unique_opts), do: Keyword.get(unique_opts, :states, [:available, :scheduled])

  defp stringify_keys(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), stringify_keys(nested)} end)
    |> Map.new()
  end

  defp stringify_keys(value) when is_list(value), do: Enum.map(value, &stringify_keys/1)
  defp stringify_keys(value), do: value
end
