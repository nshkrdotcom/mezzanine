defmodule Mezzanine.EvidenceLedger.Summary do
  @moduledoc """
  Read helpers over the durable evidence ledger for current-state completeness checks.
  """

  alias Mezzanine.EvidenceLedger.EvidenceRecord

  @complete_statuses ["collected", "verified"]

  @spec for_subject(Ecto.UUID.t()) :: [map()]
  def for_subject(subject_id) do
    case EvidenceRecord.for_subject(subject_id) do
      {:ok, records} ->
        records
        |> latest_by_kind()
        |> Map.values()

      {:error, _error} ->
        []
    end
  end

  @spec completeness(Ecto.UUID.t(), Ecto.UUID.t(), [String.t()]) :: :complete | :incomplete
  def completeness(_subject_id, _execution_id, []), do: :complete

  def completeness(subject_id, execution_id, required_evidence_kinds) do
    case EvidenceRecord.for_subject_execution(subject_id, execution_id) do
      {:ok, records} ->
        records
        |> latest_by_kind()
        |> complete_for_required_kinds?(required_evidence_kinds)
        |> if(do: :complete, else: :incomplete)

      {:error, _error} ->
        :incomplete
    end
  end

  defp latest_by_kind(records) do
    records
    |> Enum.sort_by(&sort_key/1)
    |> Enum.reduce(%{}, fn record, acc ->
      Map.put(acc, record.evidence_kind, %{
        evidence_kind: record.evidence_kind,
        status: record.status
      })
    end)
  end

  defp complete_for_required_kinds?(latest_map, required_evidence_kinds) do
    Enum.all?(required_evidence_kinds, fn evidence_kind ->
      case Map.get(latest_map, evidence_kind) do
        %{status: status} -> status in @complete_statuses
        nil -> false
      end
    end)
  end

  defp sort_key(record) do
    {record.updated_at || record.inserted_at, record.inserted_at, record.id}
  end
end
