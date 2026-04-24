defmodule Mezzanine.Audit.MemoryProofToken do
  @moduledoc """
  Hash-verifiable proof token for governed-memory tier operations.
  """

  @kinds [:recall, :write_private, :share_up, :promote, :invalidate, :audit]
  @kind_map Map.new(@kinds, &{Atom.to_string(&1), &1})
  @hash_versions ["m6.v1", "m7a.v1"]
  @m7a_ordering_fields [:source_node_ref, :commit_lsn, :commit_hlc]
  @base_required_fields [
    :proof_hash_version,
    :proof_id,
    :kind,
    :tenant_ref,
    :t_event,
    :epoch_used,
    :policy_refs,
    :fragment_ids,
    :transform_hashes,
    :access_projection_hashes,
    :trace_id
  ]
  @m6_hash_fields [
    :proof_id,
    :kind,
    :tenant_ref,
    :installation_id,
    :subject_id,
    :execution_id,
    :user_ref,
    :agent_ref,
    :t_event,
    :epoch_used,
    :policy_refs,
    :fragment_ids,
    :transform_hashes,
    :access_projection_hashes,
    :trace_id,
    :parent_fragment_id,
    :child_fragment_id,
    :evidence_refs,
    :governance_decision_ref,
    :metadata
  ]
  @m7a_hash_fields [
    :proof_hash_version,
    :proof_id,
    :kind,
    :tenant_ref,
    :installation_id,
    :subject_id,
    :execution_id,
    :user_ref,
    :agent_ref,
    :t_event,
    :epoch_used,
    :source_node_ref,
    :commit_lsn,
    :commit_hlc,
    :policy_refs,
    :fragment_ids,
    :transform_hashes,
    :access_projection_hashes,
    :trace_id,
    :parent_fragment_id,
    :child_fragment_id,
    :evidence_refs,
    :governance_decision_ref,
    :metadata
  ]
  @string_list_fields [:fragment_ids, :transform_hashes, :access_projection_hashes]

  @type kind :: :recall | :write_private | :share_up | :promote | :invalidate | :audit
  @type policy_ref :: map()
  @type evidence_ref :: map()

  @enforce_keys @base_required_fields
  defstruct proof_hash_version: nil,
            proof_id: nil,
            kind: nil,
            tenant_ref: nil,
            installation_id: nil,
            subject_id: nil,
            execution_id: nil,
            user_ref: nil,
            agent_ref: nil,
            t_event: nil,
            epoch_used: nil,
            policy_refs: [],
            fragment_ids: [],
            transform_hashes: [],
            access_projection_hashes: [],
            proof_hash: nil,
            trace_id: nil,
            source_node_ref: nil,
            commit_lsn: nil,
            commit_hlc: nil,
            parent_fragment_id: nil,
            child_fragment_id: nil,
            evidence_refs: [],
            governance_decision_ref: nil,
            metadata: %{}

  @type t :: %__MODULE__{
          proof_hash_version: String.t(),
          proof_id: String.t(),
          kind: kind(),
          tenant_ref: String.t(),
          installation_id: String.t() | nil,
          subject_id: String.t() | nil,
          execution_id: String.t() | nil,
          user_ref: String.t() | nil,
          agent_ref: String.t() | nil,
          t_event: DateTime.t(),
          epoch_used: integer(),
          policy_refs: [policy_ref()],
          fragment_ids: [String.t()],
          transform_hashes: [String.t()],
          access_projection_hashes: [String.t()],
          proof_hash: String.t(),
          trace_id: String.t(),
          source_node_ref: String.t() | nil,
          commit_lsn: String.t() | nil,
          commit_hlc: map() | nil,
          parent_fragment_id: String.t() | nil,
          child_fragment_id: String.t() | nil,
          evidence_refs: [evidence_ref()],
          governance_decision_ref: map() | nil,
          metadata: map()
        }

  @spec kinds() :: [kind()]
  def kinds, do: @kinds

  @spec hash_versions() :: [String.t()]
  def hash_versions, do: @hash_versions

  @spec new(map()) ::
          {:ok, t()}
          | {:error, {:invalid_proof_hash, :proof_hash}}
          | {:error, {:invalid_proof_hash_version, term()}}
          | {:error, {:invalid_proof_token_kind, term()}}
          | {:error, {:missing_proof_token_fields, [atom()]}}
          | {:error, {:proof_hash_mismatch, map()}}
          | {:error, {:snapshot_epoch_mismatch, map()}}
          | {:error, {:version_field_mismatch, String.t(), [atom()]}}
  def new(attrs) when is_map(attrs) do
    with {:ok, normalized_attrs} <- normalize(attrs),
         :ok <- validate_versioned_fields(normalized_attrs),
         {:ok, proof_hash} <- resolve_proof_hash(normalized_attrs) do
      {:ok, struct!(__MODULE__, Map.put(normalized_attrs, :proof_hash, proof_hash))}
    end
  end

  @spec new!(map()) :: t()
  def new!(attrs) when is_map(attrs) do
    case new(attrs) do
      {:ok, token} ->
        token

      {:error, {:missing_proof_token_fields, [field | _fields]}} ->
        raise ArgumentError, "missing required proof token field: #{field}"

      {:error, {:invalid_proof_token_kind, kind}} ->
        raise ArgumentError, "invalid proof token kind: #{inspect(kind)}"

      {:error, {:invalid_proof_hash_version, version}} ->
        raise ArgumentError, "invalid proof token hash version: #{inspect(version)}"

      {:error, {:invalid_proof_hash, :proof_hash}} ->
        raise ArgumentError, "invalid proof token proof_hash"

      {:error, {:proof_hash_mismatch, details}} ->
        raise ArgumentError, "proof token proof_hash mismatch: #{inspect(details)}"

      {:error, {:snapshot_epoch_mismatch, details}} ->
        raise ArgumentError, "proof token snapshot_epoch mismatch: #{inspect(details)}"

      {:error, {:version_field_mismatch, version, fields}} ->
        raise ArgumentError,
              "proof token #{version} field mismatch: #{inspect(Enum.sort(fields))}"
    end
  end

  @spec compute_proof_hash(t()) :: String.t()
  def compute_proof_hash(%__MODULE__{} = token) do
    token
    |> Map.take(hash_fields_for(token.proof_hash_version))
    |> canonical_json()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @spec verify_hash(t() | map()) ::
          :ok | {:error, {:invalid_proof_hash, :proof_hash} | {:proof_hash_mismatch, map()}}
  def verify_hash(%__MODULE__{} = token) do
    case normalize_hash(token.proof_hash) do
      {:ok, proof_hash} ->
        expected_hash = compute_proof_hash(token)

        if proof_hash == expected_hash do
          :ok
        else
          {:error,
           {:proof_hash_mismatch,
            %{proof_id: token.proof_id, expected: expected_hash, actual: proof_hash}}}
        end

      :error ->
        {:error, {:invalid_proof_hash, :proof_hash}}
    end
  end

  def verify_hash(attrs) when is_map(attrs) do
    with {:ok, token} <- new(attrs) do
      verify_hash(token)
    end
  end

  defp normalize(attrs) do
    source =
      if is_struct(attrs) do
        Map.from_struct(attrs)
      else
        attrs
      end

    with {:ok, kind} <- normalize_kind(fetch(source, :kind)),
         {:ok, proof_hash_version} <- normalize_hash_version(fetch(source, :proof_hash_version)),
         {:ok, epoch_used} <- normalize_epoch_used(source) do
      {:ok,
       %{
         proof_hash_version: proof_hash_version,
         proof_id: normalize_string(fetch(source, :proof_id)),
         kind: kind,
         tenant_ref: normalize_string(fetch(source, :tenant_ref)),
         installation_id: normalize_string(fetch(source, :installation_id)),
         subject_id: normalize_string(fetch(source, :subject_id)),
         execution_id: normalize_string(fetch(source, :execution_id)),
         user_ref: normalize_string(fetch(source, :user_ref)),
         agent_ref: normalize_string(fetch(source, :agent_ref)),
         t_event: normalize_datetime(fetch(source, :t_event)),
         epoch_used: epoch_used,
         policy_refs: normalize_policy_refs(fetch(source, :policy_refs)),
         fragment_ids: normalize_string_list(fetch(source, :fragment_ids)),
         transform_hashes: normalize_string_list(fetch(source, :transform_hashes)),
         access_projection_hashes:
           normalize_string_list(fetch(source, :access_projection_hashes)),
         trace_id:
           normalize_string(fetch(source, :trace_id)) || normalize_trace_id_from_process_context(),
         source_node_ref: normalize_string(fetch(source, :source_node_ref)),
         commit_lsn: normalize_string(fetch(source, :commit_lsn)),
         commit_hlc: normalize_optional_map(fetch(source, :commit_hlc)),
         parent_fragment_id: normalize_string(fetch(source, :parent_fragment_id)),
         child_fragment_id: normalize_string(fetch(source, :child_fragment_id)),
         evidence_refs: normalize_map_list(fetch(source, :evidence_refs)),
         governance_decision_ref: normalize_optional_map(fetch(source, :governance_decision_ref)),
         metadata: normalize_map(fetch(source, :metadata), %{}),
         proof_hash: fetch(source, :proof_hash)
       }}
    end
  end

  defp validate_versioned_fields(attrs) do
    case validate_required_fields(attrs) do
      :ok -> validate_field_set(attrs)
      error -> error
    end
  end

  defp validate_required_fields(attrs) do
    missing_fields =
      attrs
      |> required_fields_for()
      |> Enum.filter(fn field ->
        missing_required_field?(field, Map.get(attrs, field))
      end)

    case missing_fields do
      [] -> :ok
      fields -> {:error, {:missing_proof_token_fields, fields}}
    end
  end

  defp validate_field_set(%{proof_hash_version: "m6.v1"} = attrs) do
    present_fields =
      Enum.filter(@m7a_ordering_fields, fn field ->
        not missing_required_field?(field, Map.get(attrs, field))
      end)

    case present_fields do
      [] -> :ok
      fields -> {:error, {:version_field_mismatch, "m6.v1", fields}}
    end
  end

  defp validate_field_set(%{proof_hash_version: "m7a.v1"}), do: :ok
  defp validate_field_set(_attrs), do: :ok

  defp resolve_proof_hash(attrs) do
    token = struct!(__MODULE__, Map.put(attrs, :proof_hash, nil))
    expected_hash = compute_proof_hash(token)

    case fetch(attrs, :proof_hash) do
      nil ->
        {:ok, expected_hash}

      proof_hash ->
        proof_hash
        |> normalize_hash()
        |> resolve_existing_proof_hash(expected_hash, token.proof_id)
    end
  end

  defp resolve_existing_proof_hash({:ok, expected_hash}, expected_hash, _proof_id),
    do: {:ok, expected_hash}

  defp resolve_existing_proof_hash({:ok, normalized_hash}, expected_hash, proof_id) do
    {:error,
     {:proof_hash_mismatch,
      %{proof_id: proof_id, expected: expected_hash, actual: normalized_hash}}}
  end

  defp resolve_existing_proof_hash(:error, _expected_hash, _proof_id),
    do: {:error, {:invalid_proof_hash, :proof_hash}}

  defp required_fields_for(%{proof_hash_version: "m7a.v1"}),
    do: @base_required_fields ++ @m7a_ordering_fields

  defp required_fields_for(_attrs), do: @base_required_fields

  defp hash_fields_for("m7a.v1"), do: @m7a_hash_fields
  defp hash_fields_for(_version), do: @m6_hash_fields

  defp normalize_kind(kind) when kind in @kinds, do: {:ok, kind}

  defp normalize_kind(kind) when is_binary(kind) do
    case Map.fetch(@kind_map, kind) do
      {:ok, normalized_kind} -> {:ok, normalized_kind}
      :error -> {:error, {:invalid_proof_token_kind, kind}}
    end
  end

  defp normalize_kind(nil), do: {:ok, nil}
  defp normalize_kind(kind), do: {:error, {:invalid_proof_token_kind, kind}}

  defp normalize_hash_version(version) when version in @hash_versions, do: {:ok, version}

  defp normalize_hash_version(version) when is_binary(version) do
    version
    |> String.trim()
    |> case do
      "" -> {:ok, nil}
      normalized_version when normalized_version in @hash_versions -> {:ok, normalized_version}
      normalized_version -> {:error, {:invalid_proof_hash_version, normalized_version}}
    end
  end

  defp normalize_hash_version(nil), do: {:ok, nil}
  defp normalize_hash_version(version), do: {:error, {:invalid_proof_hash_version, version}}

  defp normalize_policy_refs(policy_refs) when is_list(policy_refs) do
    policy_refs
    |> Enum.reduce_while([], fn policy_ref, acc ->
      case normalize_policy_ref(policy_ref) do
        {:ok, normalized_policy_ref} -> {:cont, [normalized_policy_ref | acc]}
        :error -> {:halt, []}
      end
    end)
    |> Enum.reverse()
  end

  defp normalize_policy_refs(_policy_refs), do: []

  defp normalize_policy_ref(policy_ref) when is_map(policy_ref) do
    policy_id = normalize_string(fetch(policy_ref, :id))
    version = normalize_integer(fetch(policy_ref, :version))

    if is_binary(policy_id) and is_integer(version) do
      {:ok, %{"id" => policy_id, "version" => version}}
    else
      :error
    end
  end

  defp normalize_policy_ref(_policy_ref), do: :error

  defp normalize_trace_id_from_process_context do
    case Process.get(:aitrace_context) do
      nil -> nil
      context -> normalize_string(fetch(context, :trace_id))
    end
  end

  defp normalize_datetime(%DateTime{} = datetime), do: datetime

  defp normalize_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _error -> nil
    end
  end

  defp normalize_datetime(_value), do: nil

  defp normalize_integer(value) when is_integer(value), do: value

  defp normalize_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _error -> nil
    end
  end

  defp normalize_integer(_value), do: nil

  defp normalize_epoch_used(source) do
    epoch_used = source |> fetch(:epoch_used) |> normalize_integer()
    snapshot_epoch = source |> fetch(:snapshot_epoch) |> normalize_integer()

    case {epoch_used, snapshot_epoch} do
      {nil, nil} ->
        {:ok, nil}

      {nil, snapshot_epoch} ->
        {:ok, snapshot_epoch}

      {epoch_used, nil} ->
        {:ok, epoch_used}

      {same_epoch, same_epoch} ->
        {:ok, same_epoch}

      {epoch_used, snapshot_epoch} ->
        {:error,
         {:snapshot_epoch_mismatch, %{epoch_used: epoch_used, snapshot_epoch: snapshot_epoch}}}
    end
  end

  defp normalize_string(value) when is_binary(value) do
    value
    |> String.trim()
    |> case do
      "" -> nil
      normalized_value -> normalized_value
    end
  end

  defp normalize_string(_value), do: nil

  defp normalize_string_list(values) when is_list(values) do
    values
    |> Enum.map(&normalize_string/1)
    |> Enum.reject(&is_nil/1)
  end

  defp normalize_string_list(_value), do: []

  defp normalize_map_list(values) when is_list(values) do
    Enum.map(values, &normalize_map(&1, %{}))
  end

  defp normalize_map_list(_value), do: []

  defp normalize_optional_map(nil), do: nil
  defp normalize_optional_map(value) when is_map(value), do: normalize_map(value, %{})
  defp normalize_optional_map(_value), do: nil

  defp normalize_map(value, _default) when is_map(value) do
    value
    |> Enum.reduce(%{}, fn {key, nested_value}, acc ->
      Map.put(acc, to_string(key), normalize_nested_value(nested_value))
    end)
  end

  defp normalize_map(_value, default), do: default

  defp normalize_nested_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_nested_value(value) when is_map(value), do: normalize_map(value, %{})

  defp normalize_nested_value(value) when is_list(value),
    do: Enum.map(value, &normalize_nested_value/1)

  defp normalize_nested_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_nested_value(value), do: value

  defp normalize_hash(hash) when is_binary(hash) do
    normalized_hash = String.downcase(String.trim(hash))

    if byte_size(normalized_hash) == 64 and String.match?(normalized_hash, ~r/\A[0-9a-f]{64}\z/) do
      {:ok, normalized_hash}
    else
      :error
    end
  end

  defp normalize_hash(_hash), do: :error

  defp missing_required_field?(:policy_refs, []), do: true
  defp missing_required_field?(:commit_hlc, value) when value == %{}, do: true
  defp missing_required_field?(field, value) when field in @string_list_fields, do: is_nil(value)
  defp missing_required_field?(_field, nil), do: true
  defp missing_required_field?(_field, _value), do: false

  defp fetch(map, key) when is_map(map) do
    case Map.fetch(map, key) do
      {:ok, value} ->
        value

      :error ->
        Map.get(map, Atom.to_string(key))
    end
  end

  defp canonical_json(%DateTime{} = value), do: Jason.encode!(DateTime.to_iso8601(value))
  defp canonical_json(true), do: "true"
  defp canonical_json(false), do: "false"
  defp canonical_json(nil), do: "null"

  defp canonical_json(value) when is_map(value) do
    entries =
      value
      |> Enum.map(fn {key, nested_value} -> {to_string(key), nested_value} end)
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(fn {key, nested_value} ->
        Jason.encode!(key) <> ":" <> canonical_json(nested_value)
      end)

    "{" <> Enum.join(entries, ",") <> "}"
  end

  defp canonical_json(values) when is_list(values) do
    "[" <> Enum.map_join(values, ",", &canonical_json/1) <> "]"
  end

  defp canonical_json(value) when is_binary(value), do: Jason.encode!(value)
  defp canonical_json(value) when is_atom(value), do: Jason.encode!(Atom.to_string(value))
  defp canonical_json(value) when is_integer(value), do: Integer.to_string(value)
  defp canonical_json(value) when is_float(value), do: :erlang.float_to_binary(value, [:compact])
end
