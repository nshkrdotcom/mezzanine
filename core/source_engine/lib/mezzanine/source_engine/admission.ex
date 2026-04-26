defmodule Mezzanine.SourceEngine.Admission do
  @moduledoc """
  Pure source-event admission and dedupe helpers.
  """

  alias Mezzanine.SourceEngine.SourceEvent

  @required_fields [
    :installation_id,
    :source_binding_id,
    :provider,
    :external_ref,
    :event_kind,
    :provider_revision,
    :payload_schema,
    :trace_id,
    :causation_id
  ]

  @spec admit(map(), MapSet.t(String.t())) ::
          {:ok, SourceEvent.t(), MapSet.t(String.t())}
          | {:duplicate, SourceEvent.t(), MapSet.t(String.t())}
          | {:error, {:missing_required, atom()}}
  def admit(attrs, seen_keys) when is_map(attrs) and is_struct(seen_keys, MapSet) do
    with :ok <- validate_required(attrs) do
      event = build_event(attrs)

      if MapSet.member?(seen_keys, event.idempotency_key) do
        {:duplicate, %{event | status: :duplicate}, seen_keys}
      else
        {:ok, event, MapSet.put(seen_keys, event.idempotency_key)}
      end
    end
  end

  defp validate_required(attrs) do
    case Enum.find(@required_fields, &blank?(value(attrs, &1))) do
      nil -> :ok
      field -> {:error, {:missing_required, field}}
    end
  end

  defp build_event(attrs) do
    normalized_payload = value(attrs, :normalized_payload) || %{}
    idempotency_key = value(attrs, :idempotency_key) || idempotency_key(attrs)
    source_event_id = "src_" <> digest(idempotency_key, 24)

    %SourceEvent{
      source_event_id: source_event_id,
      installation_id: value(attrs, :installation_id),
      source_binding_id: value(attrs, :source_binding_id),
      provider: value(attrs, :provider),
      external_ref: value(attrs, :external_ref),
      event_kind: value(attrs, :event_kind),
      provider_revision: value(attrs, :provider_revision),
      payload_schema: value(attrs, :payload_schema),
      payload_hash: "sha256:" <> digest(normalized_payload),
      idempotency_key: idempotency_key,
      trace_id: value(attrs, :trace_id),
      causation_id: value(attrs, :causation_id),
      occurred_at: value(attrs, :occurred_at),
      normalized_payload: normalized_payload,
      provider_payload_ref: value(attrs, :provider_payload_ref)
    }
  end

  defp idempotency_key(attrs) do
    [
      value(attrs, :provider),
      value(attrs, :source_binding_id),
      value(attrs, :external_ref),
      value(attrs, :event_kind),
      value(attrs, :provider_revision)
    ]
    |> Enum.join("/")
  end

  defp value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp blank?(value), do: value in [nil, ""]

  defp digest(value, length \\ 64) do
    value
    |> canonical()
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, length)
  end

  defp canonical(map) when is_map(map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), canonical(value)} end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp canonical(list) when is_list(list), do: Enum.map(list, &canonical/1)
  defp canonical(value), do: value
end
