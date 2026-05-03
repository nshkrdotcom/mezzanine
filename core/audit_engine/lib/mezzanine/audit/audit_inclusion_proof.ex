defmodule Mezzanine.Audit.AuditInclusionProof do
  @moduledoc """
  Audit-owned inclusion/checkpoint proof evidence for durable audit facts.

  Phase 5 treats immutable-audit claims as evidence-backed only. A normal audit
  fact can carry linear checkpoint evidence; Merkle inclusion must be named as a
  `merkle_tree` proof and must carry the root and sibling path needed by the
  verifier.
  """

  alias Mezzanine.Audit.AuditFact

  @proof_types ["linear_checkpoint", "merkle_tree"]
  @sibling_sides ["left", "right"]
  @default_algorithm "sha256:erlang-term-canonical-v1"
  @required_fields [
    :proof_type,
    :audit_fact_id,
    :installation_id,
    :trace_id,
    :fact_kind,
    :occurred_at,
    :fact_hash,
    :payload_hash,
    :checkpoint_ref,
    :algorithm,
    :release_manifest_ref
  ]

  @enforce_keys @required_fields
  defstruct @required_fields ++
              [
                :position,
                :sequence,
                :previous_checkpoint_hash,
                :root_hash,
                sibling_path: [],
                metadata: %{}
              ]

  @type proof_type :: String.t()

  @type t :: %__MODULE__{
          proof_type: proof_type(),
          audit_fact_id: String.t(),
          installation_id: String.t(),
          trace_id: String.t(),
          fact_kind: String.t(),
          occurred_at: DateTime.t(),
          fact_hash: String.t(),
          payload_hash: String.t(),
          position: non_neg_integer() | nil,
          sequence: non_neg_integer() | nil,
          checkpoint_ref: String.t(),
          algorithm: String.t(),
          release_manifest_ref: String.t(),
          previous_checkpoint_hash: String.t() | nil,
          root_hash: String.t() | nil,
          sibling_path: [map()],
          metadata: map()
        }

  @spec proof_types() :: [proof_type(), ...]
  def proof_types, do: @proof_types

  @spec default_algorithm() :: String.t()
  def default_algorithm, do: @default_algorithm

  @spec required_fields() :: [atom(), ...]
  def required_fields, do: @required_fields

  @spec from_fact(AuditFact.t(), map() | keyword()) :: {:ok, t()} | {:error, term()}
  def from_fact(%AuditFact{} = fact, attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    %{
      proof_type: map_value(attrs, :proof_type) || "linear_checkpoint",
      audit_fact_id: fact.id,
      installation_id: fact.installation_id,
      trace_id: fact.trace_id,
      fact_kind: fact_kind(fact.fact_kind),
      occurred_at: fact.occurred_at,
      fact_hash: fact_hash(fact),
      payload_hash: payload_hash(fact.payload),
      position: map_value(attrs, :position),
      sequence: map_value(attrs, :sequence),
      checkpoint_ref: map_value(attrs, :checkpoint_ref),
      algorithm: map_value(attrs, :algorithm) || @default_algorithm,
      release_manifest_ref: map_value(attrs, :release_manifest_ref),
      previous_checkpoint_hash: map_value(attrs, :previous_checkpoint_hash),
      root_hash: map_value(attrs, :root_hash),
      sibling_path: map_value(attrs, :sibling_path) || [],
      metadata: map_value(attrs, :metadata) || %{}
    }
    |> new()
  end

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with :ok <- validate_required(attrs),
         :ok <- validate_proof_type(map_value(attrs, :proof_type)),
         :ok <- validate_position(attrs),
         :ok <- validate_hash(map_value(attrs, :fact_hash), :fact_hash),
         :ok <- validate_hash(map_value(attrs, :payload_hash), :payload_hash),
         :ok <- validate_optional_hash(map_value(attrs, :previous_checkpoint_hash)),
         :ok <- validate_merkle_fields(attrs),
         :ok <- validate_metadata(map_value(attrs, :metadata) || %{}) do
      {:ok,
       %__MODULE__{
         proof_type: map_value(attrs, :proof_type),
         audit_fact_id: map_value(attrs, :audit_fact_id),
         installation_id: map_value(attrs, :installation_id),
         trace_id: map_value(attrs, :trace_id),
         fact_kind: map_value(attrs, :fact_kind),
         occurred_at: map_value(attrs, :occurred_at),
         fact_hash: map_value(attrs, :fact_hash),
         payload_hash: map_value(attrs, :payload_hash),
         position: map_value(attrs, :position),
         sequence: map_value(attrs, :sequence),
         checkpoint_ref: map_value(attrs, :checkpoint_ref),
         algorithm: map_value(attrs, :algorithm),
         release_manifest_ref: map_value(attrs, :release_manifest_ref),
         previous_checkpoint_hash: map_value(attrs, :previous_checkpoint_hash),
         root_hash: map_value(attrs, :root_hash),
         sibling_path: map_value(attrs, :sibling_path) || [],
         metadata: map_value(attrs, :metadata) || %{}
       }}
    end
  end

  @spec new!(map() | keyword()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, proof} -> proof
      {:error, reason} -> raise ArgumentError, "invalid audit inclusion proof: #{inspect(reason)}"
    end
  end

  @spec fact_hash(AuditFact.t() | map()) :: String.t()
  def fact_hash(%AuditFact{} = fact) do
    digest(%{
      audit_fact_id: fact.id,
      installation_id: fact.installation_id,
      subject_id: fact.subject_id,
      execution_id: fact.execution_id,
      decision_id: fact.decision_id,
      evidence_id: fact.evidence_id,
      trace_id: fact.trace_id,
      causation_id: fact.causation_id,
      fact_kind: fact_kind(fact.fact_kind),
      payload_hash: payload_hash(fact.payload),
      occurred_at: fact.occurred_at,
      idempotency_key: fact.idempotency_key
    })
  end

  def fact_hash(attrs) when is_map(attrs) do
    digest(normalize_attrs(attrs))
  end

  @spec payload_hash(map()) :: String.t()
  def payload_hash(payload) when is_map(payload), do: digest(payload)

  @spec linear_checkpoint?(t()) :: boolean()
  def linear_checkpoint?(%__MODULE__{proof_type: "linear_checkpoint"}), do: true
  def linear_checkpoint?(%__MODULE__{}), do: false

  @spec merkle_tree?(t()) :: boolean()
  def merkle_tree?(%__MODULE__{proof_type: "merkle_tree"}), do: true
  def merkle_tree?(%__MODULE__{}), do: false

  @spec merkle_root_hash(String.t(), [map()]) :: {:ok, String.t()} | {:error, term()}
  def merkle_root_hash(leaf_hash, sibling_path) do
    with :ok <- validate_hash(leaf_hash, :leaf_hash),
         :ok <- validate_sibling_path(sibling_path) do
      {:ok, Enum.reduce(sibling_path, leaf_hash, &combine_merkle_hash/2)}
    end
  end

  defp validate_required(attrs) do
    missing =
      Enum.reject(@required_fields, fn field ->
        present?(map_value(attrs, field))
      end)

    case missing do
      [] -> :ok
      _missing -> {:error, {:missing_inclusion_proof_fields, missing}}
    end
  end

  defp validate_proof_type(proof_type) when proof_type in @proof_types, do: :ok
  defp validate_proof_type(proof_type), do: {:error, {:invalid_proof_type, proof_type}}

  defp validate_position(attrs) do
    position = map_value(attrs, :position)
    sequence = map_value(attrs, :sequence)

    cond do
      is_nil(position) and is_nil(sequence) ->
        {:error, {:missing_inclusion_position, [:position, :sequence]}}

      valid_non_negative_integer?(position) and valid_non_negative_integer?(sequence) ->
        :ok

      true ->
        {:error, {:invalid_inclusion_position, %{position: position, sequence: sequence}}}
    end
  end

  defp validate_hash(value, field) when is_binary(value) and byte_size(value) == 64 do
    if lower_hex?(value) do
      :ok
    else
      {:error, {:invalid_hash, field}}
    end
  end

  defp validate_hash(_value, field), do: {:error, {:invalid_hash, field}}

  defp validate_optional_hash(nil), do: :ok
  defp validate_optional_hash(value), do: validate_hash(value, :previous_checkpoint_hash)

  defp lower_hex?(value) do
    value
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end

  defp validate_merkle_fields(attrs) do
    case map_value(attrs, :proof_type) do
      "merkle_tree" ->
        root_hash = map_value(attrs, :root_hash)

        with :ok <- validate_hash(root_hash, :root_hash),
             {:ok, recomputed_root} <-
               merkle_root_hash(map_value(attrs, :fact_hash), map_value(attrs, :sibling_path)) do
          validate_merkle_root_match(root_hash, recomputed_root)
        end

      _other ->
        :ok
    end
  end

  defp validate_sibling_path(nil), do: {:error, {:missing_sibling_path, :sibling_path}}

  defp validate_sibling_path(path) when is_list(path) do
    Enum.reduce_while(path, :ok, fn sibling, _acc ->
      case validate_sibling(sibling) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_sibling_path(path), do: {:error, {:invalid_sibling_path, path}}

  defp validate_sibling(sibling) when is_map(sibling) do
    case validate_sibling_side(sibling_side(sibling)) do
      :ok -> validate_hash(map_value(sibling, :hash), :sibling_hash)
      error -> error
    end
  end

  defp validate_sibling(sibling), do: {:error, {:invalid_sibling_path_entry, sibling}}

  defp validate_sibling_side(side) when side in @sibling_sides, do: :ok
  defp validate_sibling_side(side), do: {:error, {:invalid_sibling_side, side}}

  defp sibling_side(sibling), do: map_value(sibling, :side) || map_value(sibling, :position)

  defp combine_merkle_hash(sibling, current_hash) do
    sibling_hash = map_value(sibling, :hash)

    case sibling_side(sibling) do
      "left" -> digest(%{left: sibling_hash, right: current_hash})
      "right" -> digest(%{left: current_hash, right: sibling_hash})
    end
  end

  defp validate_merkle_root_match(root_hash, root_hash), do: :ok

  defp validate_merkle_root_match(expected, computed) do
    {:error, {:merkle_root_mismatch, %{expected: expected, computed: computed}}}
  end

  defp validate_metadata(metadata) when is_map(metadata), do: :ok
  defp validate_metadata(metadata), do: {:error, {:invalid_metadata, metadata}}

  defp valid_non_negative_integer?(nil), do: true
  defp valid_non_negative_integer?(value), do: is_integer(value) and value >= 0

  defp present?(nil), do: false
  defp present?(value) when is_binary(value), do: byte_size(value) > 0
  defp present?(%DateTime{}), do: true
  defp present?(_value), do: true

  defp normalize_attrs(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_attrs()

  defp normalize_attrs(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), normalize_value(value)} end)
  end

  defp normalize_key(key) when is_atom(key) or is_binary(key), do: key

  defp normalize_value(nil), do: nil
  defp normalize_value(%DateTime{} = datetime), do: DateTime.truncate(datetime, :microsecond)
  defp normalize_value(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp normalize_map(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
  end

  defp map_value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, to_string(key))

  defp fact_kind(value) when is_atom(value), do: Atom.to_string(value)
  defp fact_kind(value), do: value

  defp digest(value) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(canonical(value)))
    |> Base.encode16(case: :lower)
  end

  defp canonical(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp canonical(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)

  defp canonical(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), canonical(nested)} end)
    |> Enum.sort_by(fn {key, _value} -> key end)
  end

  defp canonical(value) when is_list(value), do: Enum.map(value, &canonical/1)
  defp canonical(value), do: value
end
