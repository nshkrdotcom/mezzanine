defmodule Mezzanine.Memory.MemoryCandidate.V1 do
  @moduledoc """
  Candidate contract for promoting shared memory into governed memory.

  Candidate ids are deterministic over the shared fragment and policy lineage,
  not over the proposing node. That lets the coordinator reject a same-candidate
  claim that arrives from a different source node before any governed write.
  """

  @contract_name "Mezzanine.Memory.MemoryCandidate.V1"
  @required_string_fields [
    :tenant_ref,
    :installation_ref,
    :shared_fragment_id,
    :trace_id,
    :promotion_policy_ref,
    :access_projection_hash,
    :content_hash,
    :schema_ref
  ]
  @required_list_fields [
    :source_fragment_ids,
    :source_agents,
    :source_resources,
    :source_scopes,
    :access_agents,
    :access_resources,
    :access_scopes,
    :applied_policies
  ]
  @required_non_empty_list_fields [:evidence_refs, :governance_refs]
  @required_map_fields [:content_ref, :rebuild_spec]

  @fields [
    :contract_name,
    :candidate_id,
    :tenant_ref,
    :installation_ref,
    :shared_fragment_id,
    :source_fragment_ids,
    :source_node_ref,
    :commit_lsn,
    :commit_hlc,
    :t_epoch,
    :trace_id,
    :promotion_policy_ref,
    :source_agents,
    :source_resources,
    :source_scopes,
    :access_agents,
    :access_resources,
    :access_scopes,
    :access_projection_hash,
    :applied_policies,
    :evidence_refs,
    :governance_refs,
    :content_hash,
    :content_ref,
    :schema_ref,
    :rebuild_spec,
    :quarantined?,
    :metadata
  ]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = candidate), do: {:ok, candidate}

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, source_node_ref} <- ordering_string(attrs, :source_node_ref),
         {:ok, commit_lsn} <- ordering_string(attrs, :commit_lsn),
         {:ok, commit_hlc} <- commit_hlc(Map.get(attrs, :commit_hlc)),
         {:ok, t_epoch} <- positive_integer(Map.get(attrs, :t_epoch), :t_epoch),
         :ok <- ensure_required_strings(attrs),
         :ok <- ensure_required_lists(attrs),
         :ok <- ensure_required_non_empty_lists(attrs),
         :ok <- ensure_required_maps(attrs) do
      candidate =
        struct!(__MODULE__,
          contract_name: @contract_name,
          candidate_id: candidate_id(attrs),
          tenant_ref: Map.fetch!(attrs, :tenant_ref),
          installation_ref: Map.fetch!(attrs, :installation_ref),
          shared_fragment_id: Map.fetch!(attrs, :shared_fragment_id),
          source_fragment_ids: Map.fetch!(attrs, :source_fragment_ids),
          source_node_ref: source_node_ref,
          commit_lsn: commit_lsn,
          commit_hlc: commit_hlc,
          t_epoch: t_epoch,
          trace_id: Map.fetch!(attrs, :trace_id),
          promotion_policy_ref: Map.fetch!(attrs, :promotion_policy_ref),
          source_agents: Map.fetch!(attrs, :source_agents),
          source_resources: Map.fetch!(attrs, :source_resources),
          source_scopes: Map.fetch!(attrs, :source_scopes),
          access_agents: Map.fetch!(attrs, :access_agents),
          access_resources: Map.fetch!(attrs, :access_resources),
          access_scopes: Map.fetch!(attrs, :access_scopes),
          access_projection_hash: Map.fetch!(attrs, :access_projection_hash),
          applied_policies: Map.fetch!(attrs, :applied_policies),
          evidence_refs: Map.fetch!(attrs, :evidence_refs),
          governance_refs: Map.fetch!(attrs, :governance_refs),
          content_hash: Map.fetch!(attrs, :content_hash),
          content_ref: Map.fetch!(attrs, :content_ref),
          schema_ref: Map.fetch!(attrs, :schema_ref),
          rebuild_spec: Map.fetch!(attrs, :rebuild_spec),
          quarantined?: Map.get(attrs, :quarantined?, false),
          metadata: Map.get(attrs, :metadata, %{})
        )

      {:ok, candidate}
    end
  end

  def new(_attrs), do: {:error, :invalid_memory_candidate}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, candidate} -> candidate
      {:error, reason} -> raise ArgumentError, "invalid memory candidate: #{inspect(reason)}"
    end
  end

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = candidate) do
    Map.from_struct(candidate)
  end

  defp candidate_id(attrs) do
    case Map.get(attrs, :candidate_id) do
      value when is_binary(value) and value != "" ->
        value

      _value ->
        canonical =
          attrs
          |> Map.take([
            :tenant_ref,
            :installation_ref,
            :shared_fragment_id,
            :promotion_policy_ref,
            :content_hash,
            :access_projection_hash
          ])
          |> canonical_json()

        digest =
          :sha256
          |> :crypto.hash(canonical)
          |> Base.encode16(case: :lower)

        "memory-candidate://#{digest}"
    end
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_attrs()

  defp normalize_attrs(%__MODULE__{} = candidate), do: Map.from_struct(candidate)

  defp normalize_attrs(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    case key do
      "quarantined?" -> :quarantined?
      _other -> String.to_existing_atom(key)
    end
  rescue
    ArgumentError -> key
  end

  defp ordering_string(attrs, field) do
    attrs
    |> Map.get(field)
    |> required_string()
    |> case do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:missing_ordering_evidence, field}}
    end
  end

  defp commit_hlc(value) when is_map(value) do
    {:ok,
     %{
       "w" => Map.get(value, "w") || Map.get(value, :wall_ns),
       "l" => Map.get(value, "l") || Map.get(value, :logical),
       "n" => Map.get(value, "n") || Map.get(value, :node) || Map.get(value, :source_node_ref)
     }}
  end

  defp commit_hlc(_value), do: {:error, {:missing_ordering_evidence, :commit_hlc}}

  defp positive_integer(value, _field) when is_integer(value) and value > 0, do: {:ok, value}
  defp positive_integer(_value, field), do: {:error, {:invalid_positive_integer, field}}

  defp ensure_required_strings(attrs),
    do: missing_required(attrs, @required_string_fields, &string?/1)

  defp ensure_required_lists(attrs), do: missing_required(attrs, @required_list_fields, &list?/1)

  defp ensure_required_non_empty_lists(attrs) do
    missing_required(attrs, @required_non_empty_list_fields, &non_empty_list?/1)
  end

  defp ensure_required_maps(attrs), do: missing_required(attrs, @required_map_fields, &map?/1)

  defp missing_required(attrs, fields, predicate) do
    case Enum.reject(fields, fn field -> predicate.(Map.get(attrs, field)) end) do
      [] -> :ok
      fields -> {:error, {:missing_required_fields, fields}}
    end
  end

  defp string?(value) do
    match?({:ok, _value}, required_string(value))
  end

  defp list?(value), do: is_list(value)
  defp non_empty_list?(value), do: is_list(value) and value != []
  defp map?(value), do: is_map(value)

  defp required_string(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: :error, else: {:ok, value}
  end

  defp required_string(_value), do: :error

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

  defp canonical_json(value) when is_list(value) do
    "[" <> Enum.map_join(value, ",", &canonical_json/1) <> "]"
  end

  defp canonical_json(value) when is_binary(value), do: Jason.encode!(value)
  defp canonical_json(value) when is_atom(value), do: Jason.encode!(Atom.to_string(value))
  defp canonical_json(value), do: Jason.encode!(value)
end
