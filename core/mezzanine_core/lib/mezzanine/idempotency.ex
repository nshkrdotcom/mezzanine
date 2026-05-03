defmodule Mezzanine.Idempotency do
  @moduledoc """
  Canonical idempotency key derivation for Mezzanine lineage seams.

  The root key is `idem:v1:` plus SHA-256 over deterministic canonical JSON
  containing stable identity and hash fields only. Raw payload bytes, wall-clock
  timestamps, Temporal run ids, activity attempts, and random retry counters are
  intentionally outside this helper.
  """

  @root_prefix "idem:v1:"
  @root_digest_length 64
  @correlation_contract "Mezzanine.IdempotencyCorrelationEvidence.v1"
  @correlation_algorithm "idem:v1:sha256_jcs"
  @known_child_scopes [
    "activity",
    "lower_side_effect",
    "lower_submission",
    "provider_retry"
  ]
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
  @type root_error :: {:missing_canonical_idempotency_fields, [atom()]}
  @type child_error ::
          {:invalid_canonical_idempotency_key, term()}
          | {:invalid_child_idempotency_scope, term()}
          | :missing_child_idempotency_stable_ref
  @type correlation_error ::
          {:missing_idempotency_correlation_fields, [atom()]}
          | {:idempotency_correlation_mismatch, atom(), term(), term()}
  @type error :: root_error() | child_error() | correlation_error()

  @spec root_prefix() :: String.t()
  def root_prefix, do: @root_prefix

  @spec known_child_scopes() :: [String.t()]
  def known_child_scopes, do: @known_child_scopes

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

  @spec child_key(String.t(), String.t() | atom(), term()) ::
          {:ok, String.t()} | {:error, error()}
  def child_key(canonical_idempotency_key, scope, stable_ref) do
    with {:ok, payload} <- child_payload(canonical_idempotency_key, scope, stable_ref) do
      {:ok, @root_prefix <> payload["scope"] <> ":" <> sha256(canonical_json(payload))}
    end
  end

  @spec child_key!(String.t(), String.t() | atom(), term()) :: String.t()
  def child_key!(canonical_idempotency_key, scope, stable_ref) do
    case child_key(canonical_idempotency_key, scope, stable_ref) do
      {:ok, key} -> key
      {:error, reason} -> raise ArgumentError, inspect(reason)
    end
  end

  @spec child_payload(String.t(), String.t() | atom(), term()) :: {:ok, map()} | {:error, error()}
  def child_payload(canonical_idempotency_key, scope, stable_ref) do
    with :ok <- validate_root_key(canonical_idempotency_key),
         {:ok, normalized_scope} <- normalize_scope(scope),
         {:ok, normalized_ref} <- normalize_stable_ref(stable_ref) do
      {:ok,
       %{
         "canonical_idempotency_key" => canonical_idempotency_key,
         "scope" => normalized_scope,
         "stable_ref" => normalized_ref
       }}
    end
  end

  @doc """
  Build the idempotency correlation evidence map carried by workflow boundaries.

  The map preserves the canonical root plus layer-specific aliases and child
  keys. Fields that must equal the root are validated when present. Child keys
  are derived or validated when their stable refs are available.
  """
  @spec correlation_evidence(attrs()) :: {:ok, map()} | {:error, error()}
  def correlation_evidence(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, canonical_key} <- canonical_root_from(attrs),
         :ok <- validate_root_key(canonical_key),
         :ok <- validate_correlation_required(attrs, canonical_key),
         :ok <- validate_root_equivalent_fields(attrs, canonical_key) do
      build_correlation_evidence(attrs, canonical_key)
    end
  end

  @spec correlation_evidence!(attrs()) :: map()
  def correlation_evidence!(attrs) when is_map(attrs) or is_list(attrs) do
    case correlation_evidence(attrs) do
      {:ok, evidence} -> evidence
      {:error, reason} -> raise ArgumentError, inspect(reason)
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

  defp first_value(attrs, keys) do
    Enum.find_value(keys, &value(attrs, &1))
  end

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

  defp validate_root_key(@root_prefix <> digest = key) do
    with true <- String.length(digest) == @root_digest_length,
         {:ok, _decoded} <- Base.decode16(digest, case: :mixed) do
      :ok
    else
      _invalid -> {:error, {:invalid_canonical_idempotency_key, key}}
    end
  end

  defp validate_root_key(key), do: {:error, {:invalid_canonical_idempotency_key, key}}

  defp canonical_root_from(attrs) do
    case first_value(attrs, [:canonical_idempotency_key, :idempotency_key]) do
      root when is_binary(root) and root != "" ->
        {:ok, root}

      root ->
        {:error, {:invalid_canonical_idempotency_key, root}}
    end
  end

  defp validate_correlation_required(attrs, canonical_key) do
    required = [
      {:canonical_idempotency_key, canonical_key},
      {:tenant_id, first_value(attrs, [:tenant_id, :tenant_ref])},
      {:trace_id, value(attrs, :trace_id)},
      {:causation_id, first_value(attrs, [:causation_id, :request_id])}
    ]

    missing =
      required
      |> Enum.reject(fn {_field, field_value} -> present?(field_value) end)
      |> Enum.map(fn {field, _field_value} -> field end)

    if missing == [] do
      :ok
    else
      {:error, {:missing_idempotency_correlation_fields, missing}}
    end
  end

  defp validate_root_equivalent_fields(attrs, canonical_key) do
    [
      :idempotency_key,
      :platform_envelope_idempotency_key,
      :temporal_start_idempotency_key,
      :jido_lower_activity_idempotency_key,
      :execution_plane_envelope_idempotency_key,
      :execution_plane_route_idempotency_key
    ]
    |> Enum.reduce_while(:ok, fn field, :ok ->
      case value(attrs, field) do
        nil ->
          {:cont, :ok}

        "" ->
          {:cont, :ok}

        ^canonical_key ->
          {:cont, :ok}

        actual ->
          {:halt, {:error, {:idempotency_correlation_mismatch, field, canonical_key, actual}}}
      end
    end)
  end

  defp build_correlation_evidence(attrs, canonical_key) do
    with {:ok, child_fields} <- child_correlation_fields(attrs, canonical_key) do
      base =
        %{
          "contract_name" => @correlation_contract,
          "derivation_algorithm" => @correlation_algorithm,
          "canonical_idempotency_key" => canonical_key,
          "tenant_id" => first_value(attrs, [:tenant_id, :tenant_ref]),
          "trace_id" => value(attrs, :trace_id),
          "causation_id" => first_value(attrs, [:causation_id, :request_id]),
          "client_retry_key" => value(attrs, :client_retry_key),
          "platform_envelope_idempotency_key" => value(attrs, :platform_envelope_idempotency_key),
          "mezzanine_submission_dedupe_key" => value(attrs, :mezzanine_submission_dedupe_key),
          "temporal_workflow_id" => first_value(attrs, [:temporal_workflow_id, :workflow_id]),
          "temporal_workflow_run_id" =>
            first_value(attrs, [:temporal_workflow_run_id, :workflow_run_id]),
          "temporal_start_idempotency_key" => value(attrs, :temporal_start_idempotency_key),
          "temporal_activity_call_ref" =>
            first_value(attrs, [:temporal_activity_call_ref, :activity_call_ref]),
          "temporal_activity_attempt_number" =>
            first_value(attrs, [:temporal_activity_attempt_number, :activity_attempt_number]),
          "jido_lower_activity_idempotency_key" =>
            value(attrs, :jido_lower_activity_idempotency_key),
          "jido_lower_submission_dedupe_key" => value(attrs, :jido_lower_submission_dedupe_key),
          "lower_provider_retry_key" =>
            first_value(attrs, [:lower_provider_retry_key, :provider_retry_key]),
          "execution_plane_intent_id" =>
            first_value(attrs, [:execution_plane_intent_id, :intent_id]),
          "execution_plane_route_id" =>
            first_value(attrs, [:execution_plane_route_id, :route_id]),
          "execution_plane_envelope_idempotency_key" =>
            value(attrs, :execution_plane_envelope_idempotency_key),
          "execution_plane_route_idempotency_key" =>
            value(attrs, :execution_plane_route_idempotency_key),
          "release_manifest_ref" => value(attrs, :release_manifest_ref)
        }
        |> Map.merge(child_fields)

      {:ok, compact_evidence(base)}
    end
  end

  defp child_correlation_fields(attrs, canonical_key) do
    with {:ok, activity_key} <-
           child_correlation_key(
             attrs,
             canonical_key,
             :activity,
             :temporal_activity_side_effect_key,
             [:activity_side_effect_key],
             first_value(attrs, [:temporal_activity_call_ref, :activity_call_ref])
           ),
         {:ok, lower_submission_key} <-
           child_correlation_key(
             attrs,
             canonical_key,
             :lower_submission,
             :mezzanine_submission_dedupe_key,
             [:jido_lower_submission_dedupe_key, :submission_dedupe_key],
             first_value(attrs, [:lower_submission_stable_ref, :lower_submission_ref])
           ),
         {:ok, provider_retry_key} <-
           child_correlation_key(
             attrs,
             canonical_key,
             :provider_retry,
             :lower_provider_retry_key,
             [:provider_retry_key],
             first_value(attrs, [:lower_provider_retry_stable_ref, :provider_retry_stable_ref])
           ) do
      {:ok,
       %{
         "temporal_activity_side_effect_key" => activity_key,
         "mezzanine_submission_dedupe_key" => lower_submission_key,
         "jido_lower_submission_dedupe_key" => lower_submission_key,
         "lower_provider_retry_key" => provider_retry_key
       }}
    end
  end

  defp child_correlation_key(attrs, canonical_key, scope, primary_field, alias_fields, stable_ref) do
    with {:ok, provided} <- provided_child_key(attrs, primary_field, alias_fields) do
      derive_child_correlation_key(provided, stable_ref, canonical_key, scope, primary_field)
    end
  end

  defp derive_child_correlation_key(provided, stable_ref, canonical_key, scope, primary_field) do
    cond do
      present?(provided) and present?(stable_ref) ->
        validate_child_correlation_stable_ref(
          provided,
          stable_ref,
          canonical_key,
          scope,
          primary_field
        )

      present?(provided) ->
        {:ok, provided}

      present?(stable_ref) ->
        child_key(canonical_key, scope, stable_ref)

      true ->
        {:ok, nil}
    end
  end

  defp validate_child_correlation_stable_ref(
         provided,
         stable_ref,
         canonical_key,
         scope,
         primary_field
       ) do
    with {:ok, expected} <- child_key(canonical_key, scope, stable_ref) do
      validate_child_correlation_key(primary_field, provided, expected)
    end
  end

  defp provided_child_key(attrs, primary_field, alias_fields) do
    values =
      [primary_field | alias_fields]
      |> Enum.map(&value(attrs, &1))
      |> Enum.filter(&present?/1)
      |> Enum.uniq()

    case values do
      [] -> {:ok, nil}
      [provided] -> {:ok, provided}
      [expected, actual | _extra] -> child_alias_mismatch(primary_field, expected, actual)
    end
  end

  defp validate_child_correlation_key(_field, provided, provided), do: {:ok, provided}

  defp validate_child_correlation_key(field, provided, expected),
    do: child_alias_mismatch(field, expected, provided)

  defp child_alias_mismatch(field, expected, actual),
    do: {:error, {:idempotency_correlation_mismatch, field, expected, actual}}

  defp compact_evidence(evidence) do
    evidence
    |> Enum.reject(fn {_key, field_value} -> not present?(field_value) end)
    |> Map.new()
  end

  defp normalize_scope(scope) when is_atom(scope),
    do: scope |> Atom.to_string() |> normalize_scope()

  defp normalize_scope(scope) when is_binary(scope) do
    if scope_name?(scope) do
      {:ok, scope}
    else
      {:error, {:invalid_child_idempotency_scope, scope}}
    end
  end

  defp normalize_scope(scope), do: {:error, {:invalid_child_idempotency_scope, scope}}

  defp scope_name?(<<first, rest::binary>>) do
    lower_alnum?(first) and
      rest
      |> :binary.bin_to_list()
      |> Enum.all?(fn byte -> lower_alnum?(byte) or byte in [?_, ?., ?-] end)
  end

  defp scope_name?(_scope), do: false

  defp lower_alnum?(byte), do: byte in ?a..?z or byte in ?0..?9

  defp normalize_stable_ref(stable_ref) do
    stable_ref = canonical_value(normalize_value(stable_ref))

    if present?(stable_ref) do
      {:ok, stable_ref}
    else
      {:error, :missing_child_idempotency_stable_ref}
    end
  end

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
