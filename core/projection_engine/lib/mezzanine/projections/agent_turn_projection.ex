defmodule Mezzanine.Projections.AgentTurnProjection do
  @moduledoc """
  ProjectionEngine adapter for product-safe agent-turn rows.
  """

  alias Mezzanine.AgentTurnEngine.Projection.Row
  alias Mezzanine.Projections.ProjectionRow

  @projection_name "agent_turn_timeline"

  @spec upsert_rows([Row.t()], map()) :: {:ok, [ProjectionRow.t()]} | {:error, term()}
  def upsert_rows(rows, attrs) when is_list(rows) and is_map(attrs) do
    rows
    |> Enum.reduce_while({:ok, []}, fn row, {:ok, acc} ->
      case ProjectionRow.upsert(to_upsert_attrs(row, attrs)) do
        {:ok, projection_row} -> {:cont, {:ok, [projection_row | acc]}}
        {:error, error} -> {:halt, {:error, error}}
      end
    end)
    |> case do
      {:ok, projection_rows} -> {:ok, Enum.reverse(projection_rows)}
      {:error, error} -> {:error, error}
    end
  end

  @spec to_upsert_attrs(Row.t(), map()) :: map()
  def to_upsert_attrs(%Row{} = row, attrs) when is_map(attrs) do
    %{
      installation_id: fetch_required!(attrs, :installation_id),
      projection_name: map_value(attrs, :projection_name) || @projection_name,
      row_key: row.row_ref,
      subject_id: map_value(attrs, :subject_id),
      execution_id: map_value(attrs, :execution_id),
      projection_kind: "agent_turn",
      sort_key: row.seq,
      trace_id: fetch_required!(attrs, :trace_id),
      causation_id: map_value(attrs, :causation_id) || row.event_ref,
      payload: payload(row, attrs),
      computed_at: row.occurred_at
    }
  end

  defp payload(%Row{} = row, attrs) do
    Map.merge(
      %{
        "row_ref" => row.row_ref,
        "ledger_ref" => row.ledger_ref,
        "seq" => row.seq,
        "event_ref" => row.event_ref,
        "event_type" => Atom.to_string(row.event_type),
        "visibility" => Atom.to_string(row.visibility),
        "summary" => row.summary,
        "payload_ref" => row.payload_ref,
        "redaction_class" => Atom.to_string(row.redaction_class),
        "authority_ref" => row.authority_ref,
        "evidence_refs" => row.evidence_refs
      },
      map_value(attrs, :payload) || %{}
    )
  end

  defp fetch_required!(attrs, key) do
    case map_value(attrs, key) do
      nil -> raise ArgumentError, "missing required #{key}"
      value -> value
    end
  end

  defp map_value(attrs, key) when is_atom(key),
    do: Map.get(attrs, key, Map.get(attrs, Atom.to_string(key)))
end
