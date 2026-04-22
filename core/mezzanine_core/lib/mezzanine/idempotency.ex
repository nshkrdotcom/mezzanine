defmodule Mezzanine.Idempotency do
  @moduledoc """
  Canonical idempotency key derivation for Mezzanine lineage seams.

  The root key is `idem:v1:` plus SHA-256 over deterministic canonical JSON
  containing stable identity and hash fields only. Raw payload bytes, wall-clock
  timestamps, Temporal run ids, activity attempts, and random retry counters are
  intentionally outside this helper.
  """

  @root_prefix "idem:v1:"
  @required_root_fields [
    :tenant_id,
    :operation_family,
    :operation_ref,
    :causation_id,
    :authority_decision_ref_or_hash,
    :subject_ref_or_resource_ref,
    :payload_hash
  ]

  @type attrs :: map() | keyword()
  @type error :: {:missing_canonical_idempotency_fields, [atom()]}

  @spec root_prefix() :: String.t()
  def root_prefix, do: @root_prefix

  @spec canonical_key(attrs()) :: {:ok, String.t()} | {:error, error()}
  def canonical_key(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, payload} <- canonical_payload(attrs) do
      {:ok, @root_prefix <> sha256(canonical_json(payload))}
    end
  end

  @spec canonical_key!(attrs()) :: String.t()
  def canonical_key!(attrs) when is_map(attrs) or is_list(attrs) do
    case canonical_key(attrs) do
      {:ok, key} -> key
      {:error, reason} -> raise ArgumentError, inspect(reason)
    end
  end

  @spec canonical_payload(attrs()) :: {:ok, map()} | {:error, error()}
  def canonical_payload(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    payload = %{
      "tenant_id" => value(attrs, :tenant_id),
      "installation_id" => value(attrs, :installation_id),
      "operation_family" => value(attrs, :operation_family),
      "operation_ref" => value(attrs, :operation_ref),
      "causation_id" => value(attrs, :causation_id),
      "authority_decision_ref" => authority_decision_ref_or_hash(attrs),
      "subject_ref" => subject_ref_or_resource_ref(attrs),
      "payload_hash" => value(attrs, :payload_hash),
      "source_event_position" => value(attrs, :source_event_position)
    }

    case missing_fields(payload) do
      [] -> {:ok, canonical_value(payload)}
      missing -> {:error, {:missing_canonical_idempotency_fields, missing}}
    end
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_attrs()

  defp normalize_attrs(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, nested} -> {normalize_key(key), normalize_value(nested)} end)
  end

  defp normalize_key(key) when is_atom(key) or is_binary(key), do: key

  defp normalize_value(nil), do: nil
  defp normalize_value(value) when is_boolean(value), do: value
  defp normalize_value(%_{} = value), do: value |> Map.from_struct() |> normalize_value()
  defp normalize_value(value) when is_map(value), do: normalize_attrs(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp authority_decision_ref_or_hash(attrs) do
    value(attrs, :authority_decision_ref) ||
      value(attrs, :authority_decision_hash)
  end

  defp subject_ref_or_resource_ref(attrs) do
    value(attrs, :subject_ref) ||
      value(attrs, :subject_id) ||
      value(attrs, :resource_ref) ||
      value(attrs, :resource_id)
  end

  defp missing_fields(payload) do
    Enum.reduce(@required_root_fields, [], fn field, missing ->
      if present?(required_value(payload, field)), do: missing, else: [field | missing]
    end)
    |> Enum.reverse()
  end

  defp required_value(payload, :authority_decision_ref_or_hash),
    do: Map.fetch!(payload, "authority_decision_ref")

  defp required_value(payload, :subject_ref_or_resource_ref),
    do: Map.fetch!(payload, "subject_ref")

  defp required_value(payload, field), do: Map.fetch!(payload, Atom.to_string(field))

  defp present?(nil), do: false
  defp present?(""), do: false
  defp present?(value) when is_list(value), do: value != []
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(_value), do: true

  defp canonical_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp canonical_value(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)

  defp canonical_value(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {to_string(key), canonical_value(nested)} end)
  end

  defp canonical_value(value) when is_list(value), do: Enum.map(value, &canonical_value/1)
  defp canonical_value(value), do: value

  defp canonical_json(value), do: value |> encode_json_value() |> IO.iodata_to_binary()

  defp encode_json_value(nil), do: "null"
  defp encode_json_value(true), do: "true"
  defp encode_json_value(false), do: "false"
  defp encode_json_value(value) when is_binary(value), do: [?\", escape_string(value), ?\"]
  defp encode_json_value(value) when is_integer(value), do: Integer.to_string(value)

  defp encode_json_value(value) when is_float(value) do
    :erlang.float_to_binary(value, [:short, :compact])
  end

  defp encode_json_value(value) when is_list(value) do
    [?[, value |> Enum.map(&encode_json_value/1) |> Enum.intersperse(","), ?]]
  end

  defp encode_json_value(value) when is_map(value) do
    entries =
      value
      |> Enum.map(fn {key, nested} -> {to_string(key), nested} end)
      |> Enum.sort_by(fn {key, _nested} -> key end)
      |> Enum.map(fn {key, nested} ->
        [encode_json_value(key), ?:, encode_json_value(nested)]
      end)

    [?{, Enum.intersperse(entries, ","), ?}]
  end

  defp escape_string(<<>>), do: []
  defp escape_string(<<"\"", rest::binary>>), do: [?\\, ?", escape_string(rest)]
  defp escape_string(<<"\\", rest::binary>>), do: [?\\, ?\\, escape_string(rest)]
  defp escape_string(<<"\b", rest::binary>>), do: [?\\, ?b, escape_string(rest)]
  defp escape_string(<<"\f", rest::binary>>), do: [?\\, ?f, escape_string(rest)]
  defp escape_string(<<"\n", rest::binary>>), do: [?\\, ?n, escape_string(rest)]
  defp escape_string(<<"\r", rest::binary>>), do: [?\\, ?r, escape_string(rest)]
  defp escape_string(<<"\t", rest::binary>>), do: [?\\, ?t, escape_string(rest)]

  defp escape_string(<<char::utf8, rest::binary>>) when char in 0..0x1F do
    ["\\u", char |> Integer.to_string(16) |> String.pad_leading(4, "0"), escape_string(rest)]
  end

  defp escape_string(<<char::utf8, rest::binary>>), do: [<<char::utf8>>, escape_string(rest)]

  defp sha256(bytes) do
    :sha256
    |> :crypto.hash(bytes)
    |> Base.encode16(case: :lower)
  end
end
