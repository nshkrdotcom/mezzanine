defmodule Mezzanine.Audit.ExecutionLineageStore do
  @moduledoc """
  Persistence adapter that round-trips the frozen `ExecutionLineage` contract.
  """

  alias Mezzanine.Audit.{ExecutionLineage, ExecutionLineageRecord}

  @spec store(ExecutionLineage.t()) :: {:ok, ExecutionLineage.t()} | {:error, term()}
  def store(%ExecutionLineage{} = lineage) do
    lineage
    |> Map.from_struct()
    |> ExecutionLineageRecord.store()
    |> case do
      {:ok, %ExecutionLineageRecord{} = record} -> to_contract(record)
      {:error, error} -> {:error, error}
    end
  end

  @spec fetch(String.t()) :: {:ok, ExecutionLineage.t()} | {:error, term()}
  def fetch(execution_id) when is_binary(execution_id) do
    case ExecutionLineageRecord.by_execution_id(execution_id) do
      {:ok, %ExecutionLineageRecord{} = record} ->
        to_contract(record)

      {:error, error} ->
        {:error, error}
    end
  end

  @spec list_trace(String.t(), String.t()) :: {:ok, [ExecutionLineage.t()]} | {:error, term()}
  def list_trace(installation_id, trace_id)
      when is_binary(installation_id) and is_binary(trace_id) do
    with {:ok, records} <- ExecutionLineageRecord.list_trace(installation_id, trace_id) do
      to_contracts(records, [])
    end
  end

  defp to_contracts([], lineages), do: {:ok, Enum.reverse(lineages)}

  defp to_contracts([record | records], lineages) do
    case to_contract(record) do
      {:ok, lineage} -> to_contracts(records, [lineage | lineages])
      {:error, error} -> {:error, error}
    end
  end

  defp to_contract(%ExecutionLineageRecord{} = record) do
    record
    |> Map.take([
      :trace_id,
      :causation_id,
      :tenant_id,
      :installation_id,
      :subject_id,
      :execution_id,
      :citadel_request_id,
      :citadel_submission_id,
      :ji_submission_key,
      :lower_run_id,
      :lower_attempt_id,
      :artifact_refs
    ])
    |> ExecutionLineage.new()
  end
end
