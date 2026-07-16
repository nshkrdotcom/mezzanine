defmodule Mezzanine.Projections.Store.AshPostgres do
  @moduledoc "Adapter-local durable projection store backed by AshPostgres."
  @behaviour Mezzanine.Projections.Store

  import Ecto.Query, only: [from: 2]

  alias Ecto.Adapters.SQL
  alias Mezzanine.Projections.Repo

  @migration_version 20_260_517_121_000

  def capabilities, do: Mezzanine.Persistence.postgres_capability(:projections, [:projections])

  def preflight(opts) do
    selected_repo = configured_repo(opts)

    with {:ok, %{rows: [[1]]}} <- SQL.query(selected_repo, "SELECT 1", []),
         {:ok, %{rows: [[true]]}} <-
           SQL.query(
             selected_repo,
             "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)",
             [@migration_version]
           ) do
      :ok
    else
      {:ok, %{rows: [[false]]}} -> {:error, {:required_migration_missing, @migration_version}}
      {:error, reason} -> {:error, {:postgres_unavailable, reason}}
      other -> {:error, {:postgres_preflight_failed, other}}
    end
  end

  def repo, do: Mezzanine.Projections.Repo

  def resource_modules,
    do: [Mezzanine.Projections.ProjectionRow, Mezzanine.Projections.MaterializedProjection]

  def health(opts) do
    with :ok <- preflight(opts) do
      {:ok,
       %{
         adapter: :ash_postgres,
         capability: capabilities(),
         migration_version: @migration_version,
         repo: configured_repo(opts),
         restart_safe?: true,
         tier: :postgres_shared
       }}
    end
  end

  def put_record(attrs, opts) when is_map(attrs) and is_list(opts) do
    with :ok <- preflight(opts) do
      Repo.transaction(fn ->
        record = attrs |> normalize_keys() |> normalize_record(opts)
        now = now()

        Repo.insert_all(
          "projection_outbox_records",
          [record_row(record, now)],
          conflict_target: [:id],
          on_conflict:
            {:replace,
             [
               :projection_ref,
               :operation_context_ref,
               :subject_ref,
               :trace_ref,
               :attrs,
               :updated_at
             ]}
        )

        record.id
        |> events_query()
        |> Repo.delete_all()

        Map.put(record, :events, [])
      end)
      |> transaction_result()
    end
  end

  def fetch_record(id, opts) when is_list(opts) do
    case preflight(opts) do
      :ok -> fetch_record_with_events(normalized_id(id))
      {:error, reason} -> {:error, reason}
    end
  end

  def update_record(id, attrs, opts) when is_map(attrs) and is_list(opts) do
    with :ok <- preflight(opts),
         {:ok, existing} <- fetch_record_with_events(normalized_id(id)) do
      Repo.transaction(fn ->
        events = Map.get(existing, :events, [])

        updated =
          existing
          |> Map.delete(:events)
          |> Map.merge(normalize_keys(attrs))
          |> Map.put(:id, normalized_id(id))

        Repo.update_all(
          record_query(updated.id),
          set: [
            projection_ref: field_value(updated, :projection_ref),
            operation_context_ref: field_value(updated, :operation_context_ref),
            subject_ref: field_value(updated, :subject_ref),
            trace_ref: field_value(updated, :trace_ref),
            attrs: encode_term(updated),
            updated_at: now()
          ]
        )

        Map.put(updated, :events, events)
      end)
      |> transaction_result()
    end
  end

  def append_event(id, event, opts) when is_map(event) and is_list(opts) do
    with :ok <- preflight(opts) do
      normalized_id(id)
      |> append_event_transaction(event)
      |> transaction_result()
    end
  end

  defp append_event_transaction(record_id, event) do
    Repo.transaction(fn ->
      case fetch_record_row(record_id) do
        {:ok, _record} -> insert_event(record_id, event)
        {:error, :not_found} -> Repo.rollback(:not_found)
      end
    end)
  end

  defp insert_event(record_id, event) do
    sequence = next_event_sequence(record_id)

    event =
      event
      |> normalize_keys()
      |> Map.put(:sequence, sequence)
      |> Map.put_new(:record_id, record_id)

    Repo.insert_all("projection_outbox_events", [event_row(record_id, event, sequence)])
    event
  end

  defp fetch_record_with_events(id) do
    with {:ok, record} <- fetch_record_row(id) do
      {:ok, Map.put(record, :events, fetch_events(id))}
    end
  end

  defp fetch_record_row(id) do
    query =
      from(record in "projection_outbox_records",
        where: field(record, :id) == ^id,
        select: %{attrs: field(record, :attrs)}
      )

    case Repo.one(query) do
      %{attrs: attrs} -> {:ok, decode_term(attrs)}
      nil -> {:error, :not_found}
    end
  end

  defp fetch_events(record_id) do
    query =
      from(event in "projection_outbox_events",
        where: field(event, :record_id) == ^record_id,
        order_by: [asc: field(event, :sequence)],
        select: %{payload: field(event, :payload)}
      )

    query
    |> Repo.all()
    |> Enum.map(&decode_term(&1.payload))
  end

  defp next_event_sequence(record_id) do
    query =
      from(event in "projection_outbox_events",
        where: field(event, :record_id) == ^record_id,
        select: max(field(event, :sequence))
      )

    case Repo.one(query) do
      nil -> 1
      sequence -> sequence + 1
    end
  end

  defp record_query(id),
    do: from(record in "projection_outbox_records", where: field(record, :id) == ^id)

  defp events_query(record_id),
    do: from(event in "projection_outbox_events", where: field(event, :record_id) == ^record_id)

  defp record_row(record, timestamp) do
    %{
      id: record.id,
      projection_ref: field_value(record, :projection_ref),
      operation_context_ref: field_value(record, :operation_context_ref),
      subject_ref: field_value(record, :subject_ref),
      trace_ref: field_value(record, :trace_ref),
      attrs: encode_term(Map.delete(record, :events)),
      inserted_at: timestamp,
      updated_at: timestamp
    }
  end

  defp event_row(record_id, event, sequence) do
    timestamp = now()

    %{
      record_id: record_id,
      sequence: sequence,
      event_ref: field_value(event, :event_ref),
      trace_ref: field_value(event, :trace_ref),
      event_kind: event_kind(event),
      causal_order: field_value(event, :causal_order),
      payload: encode_term(event),
      inserted_at: timestamp,
      updated_at: timestamp
    }
  end

  defp normalize_record(attrs, opts) do
    id =
      attrs
      |> field_value(:id)
      |> Kernel.||(Keyword.get(opts, :id))
      |> Kernel.||("projection_outbox:#{System.unique_integer([:positive, :monotonic])}")
      |> normalized_id()

    attrs
    |> Map.delete("events")
    |> Map.delete(:events)
    |> Map.put(:id, id)
  end

  defp normalize_keys(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {string_key(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp string_key("events"), do: :events
  defp string_key("id"), do: :id
  defp string_key("record_id"), do: :record_id
  defp string_key("sequence"), do: :sequence
  defp string_key(key), do: key

  defp normalized_id(id) when is_binary(id), do: id
  defp normalized_id(id), do: inspect(id)

  defp field_value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp event_kind(event) do
    case field_value(event, :event_kind) do
      kind when is_atom(kind) -> Atom.to_string(kind)
      kind -> kind
    end
  end

  defp encode_term(term), do: :erlang.term_to_binary(term)
  defp decode_term(binary), do: :erlang.binary_to_term(binary, [:safe])

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)

  defp transaction_result({:ok, value}), do: {:ok, value}
  defp transaction_result({:error, reason}), do: {:error, reason}

  defp configured_repo(opts) when is_list(opts), do: Keyword.get(opts, :repo, repo())
  defp configured_repo(opts) when is_map(opts), do: Map.get(opts, :repo, repo())
end
