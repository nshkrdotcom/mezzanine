defmodule Mezzanine.Memory.MemoryPromotionDecision.V1 do
  @moduledoc """
  Promotion decision contract for the shared-to-governed memory transition.
  """

  @contract_name "Mezzanine.Memory.MemoryPromotionDecision.V1"
  @decisions [:approved, :denied]
  @decision_sources [:auto_decide, :review]

  @fields [
    :contract_name,
    :decision_id,
    :candidate_id,
    :promotion_policy_ref,
    :decision,
    :decision_source,
    :source_node_ref,
    :commit_lsn,
    :commit_hlc,
    :review_refs,
    :evidence_refs,
    :governance_refs,
    :reason,
    :decided_at,
    :metadata
  ]

  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec new(map() | keyword() | t()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = decision), do: {:ok, decision}

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, decision} <- normalize_decision(Map.get(attrs, :decision)),
         {:ok, decision_source} <- normalize_decision_source(Map.get(attrs, :decision_source)),
         {:ok, source_node_ref} <- required_string(attrs, :source_node_ref),
         {:ok, commit_lsn} <- required_string(attrs, :commit_lsn),
         {:ok, commit_hlc} <- commit_hlc(Map.get(attrs, :commit_hlc)),
         :ok <- ensure_required_strings(attrs, [:candidate_id, :promotion_policy_ref]) do
      {:ok,
       struct!(__MODULE__,
         contract_name: @contract_name,
         decision_id: decision_id(attrs, decision),
         candidate_id: Map.fetch!(attrs, :candidate_id),
         promotion_policy_ref: Map.fetch!(attrs, :promotion_policy_ref),
         decision: decision,
         decision_source: decision_source,
         source_node_ref: source_node_ref,
         commit_lsn: commit_lsn,
         commit_hlc: commit_hlc,
         review_refs: Map.get(attrs, :review_refs, []),
         evidence_refs: Map.get(attrs, :evidence_refs, []),
         governance_refs: Map.get(attrs, :governance_refs, []),
         reason: Map.get(attrs, :reason),
         decided_at: Map.get(attrs, :decided_at, DateTime.utc_now()),
         metadata: Map.get(attrs, :metadata, %{})
       )}
    end
  end

  def new(_attrs), do: {:error, :invalid_memory_promotion_decision}

  @spec new!(map() | keyword() | t()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, decision} -> decision
      {:error, reason} -> raise ArgumentError, "invalid promotion decision: #{inspect(reason)}"
    end
  end

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = decision) do
    %{
      "contract_name" => decision.contract_name,
      "decision_id" => decision.decision_id,
      "candidate_id" => decision.candidate_id,
      "promotion_policy_ref" => decision.promotion_policy_ref,
      "decision" => Atom.to_string(decision.decision),
      "decision_source" => Atom.to_string(decision.decision_source),
      "source_node_ref" => decision.source_node_ref,
      "commit_lsn" => decision.commit_lsn,
      "commit_hlc" => decision.commit_hlc,
      "review_refs" => decision.review_refs,
      "evidence_refs" => decision.evidence_refs,
      "governance_refs" => decision.governance_refs,
      "reason" => decision.reason,
      "decided_at" => DateTime.to_iso8601(decision.decided_at),
      "metadata" => decision.metadata
    }
  end

  defp decision_id(attrs, decision) do
    case Map.get(attrs, :decision_id) do
      value when is_binary(value) and value != "" ->
        value

      _value ->
        digest =
          %{
            candidate_id: Map.fetch!(attrs, :candidate_id),
            promotion_policy_ref: Map.fetch!(attrs, :promotion_policy_ref),
            decision: decision,
            review_refs: Map.get(attrs, :review_refs, [])
          }
          |> canonical_json()
          |> then(&:crypto.hash(:sha256, &1))
          |> Base.encode16(case: :lower)

        "promotion-decision://#{digest}"
    end
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_attrs()
  defp normalize_attrs(%__MODULE__{} = decision), do: Map.from_struct(decision)

  defp normalize_attrs(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp normalize_decision(decision) when decision in @decisions, do: {:ok, decision}
  defp normalize_decision(:approve), do: {:ok, :approved}
  defp normalize_decision(:accept), do: {:ok, :approved}
  defp normalize_decision(:deny), do: {:ok, :denied}
  defp normalize_decision(:reject), do: {:ok, :denied}
  defp normalize_decision("approved"), do: {:ok, :approved}
  defp normalize_decision("approve"), do: {:ok, :approved}
  defp normalize_decision("accept"), do: {:ok, :approved}
  defp normalize_decision("denied"), do: {:ok, :denied}
  defp normalize_decision("deny"), do: {:ok, :denied}
  defp normalize_decision("reject"), do: {:ok, :denied}
  defp normalize_decision(decision), do: {:error, {:invalid_promotion_decision, decision}}

  defp normalize_decision_source(source) when source in @decision_sources, do: {:ok, source}
  defp normalize_decision_source("auto_decide"), do: {:ok, :auto_decide}
  defp normalize_decision_source("review"), do: {:ok, :review}

  defp normalize_decision_source(source),
    do: {:error, {:invalid_promotion_decision_source, source}}

  defp commit_hlc(value) when is_map(value) do
    {:ok,
     %{
       "w" => Map.get(value, "w") || Map.get(value, :wall_ns),
       "l" => Map.get(value, "l") || Map.get(value, :logical),
       "n" => Map.get(value, "n") || Map.get(value, :node) || Map.get(value, :source_node_ref)
     }}
  end

  defp commit_hlc(_value), do: {:error, {:missing_ordering_evidence, :commit_hlc}}

  defp ensure_required_strings(attrs, fields) do
    case Enum.reject(fields, fn field -> match?({:ok, _value}, required_string(attrs, field)) end) do
      [] -> :ok
      fields -> {:error, {:missing_required_fields, fields}}
    end
  end

  defp required_string(attrs, field) do
    case Map.get(attrs, field) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: {:error, {:missing_required_fields, [field]}}, else: {:ok, value}

      _value ->
        {:error, {:missing_required_fields, [field]}}
    end
  end

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
  defp canonical_json(%DateTime{} = value), do: Jason.encode!(DateTime.to_iso8601(value))
  defp canonical_json(value), do: Jason.encode!(value)
end
