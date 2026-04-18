defmodule Mezzanine.Telemetry do
  @moduledoc """
  Canonical Stage 11 telemetry namespace and metadata normalizer for Mezzanine.

  All substrate-owned telemetry emits under the `[:mezzanine, ...]` namespace
  and carries a stable dotted `event_name` in metadata so metrics, traces, and
  operator tooling can join on one contract.
  """

  @prefix [:mezzanine]
  @common_metadata_keys [
    :trace_id,
    :subject_id,
    :execution_id,
    :decision_id,
    :submission_dedupe_key,
    :lease_id,
    :tenant_id,
    :installation_id
  ]

  @type event_segment :: atom() | String.t()
  @type event_name :: [event_segment()]

  @spec emit(event_name(), map(), map()) :: :ok
  def emit(event, measurements \\ %{}, metadata \\ %{})
      when is_list(event) and is_map(measurements) and is_map(metadata) do
    normalized_event = normalize_event(event)
    normalized_measurements = normalize_measurements(measurements)

    normalized_metadata =
      metadata
      |> normalize_metadata()
      |> Map.put_new(:event_name, dotted_event_name(normalized_event))

    :telemetry.execute(
      prefixed_event_name(normalized_event),
      normalized_measurements,
      normalized_metadata
    )
  end

  @spec prefixed_event_name(event_name()) :: [atom()]
  def prefixed_event_name(event) when is_list(event), do: @prefix ++ normalize_event(event)

  @spec dotted_event_name(event_name()) :: String.t()
  def dotted_event_name(event) when is_list(event) do
    event
    |> normalize_event()
    |> Enum.map_join(".", &Atom.to_string/1)
  end

  @spec monotonic_duration_ms(integer(), integer()) :: non_neg_integer()
  def monotonic_duration_ms(start_native, stop_native \\ System.monotonic_time())
      when is_integer(start_native) and is_integer(stop_native) do
    (stop_native - start_native)
    |> System.convert_time_unit(:native, :millisecond)
    |> max(0)
  end

  defp normalize_event(event) do
    Enum.map(event, fn
      segment when is_atom(segment) ->
        segment

      segment when is_binary(segment) ->
        segment
        |> String.trim()
        |> String.replace("-", "_")
        |> String.to_atom()
    end)
  end

  defp normalize_measurements(measurements) do
    measurements
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp normalize_metadata(metadata) do
    metadata
    |> Enum.map(fn {key, value} -> {normalize_key(key), normalize_value(value)} end)
    |> Map.new()
    |> with_common_keys()
  end

  defp with_common_keys(metadata) do
    Enum.reduce(@common_metadata_keys, metadata, fn key, acc -> Map.put_new(acc, key, nil) end)
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    key
    |> String.trim()
    |> String.replace("-", "_")
  end

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_value(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)

  defp normalize_value(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {normalize_key(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)

  defp normalize_value(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&normalize_value/1)

  defp normalize_value(value), do: value
end
