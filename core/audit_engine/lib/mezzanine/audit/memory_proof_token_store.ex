defmodule Mezzanine.Audit.MemoryProofTokenStore do
  @moduledoc """
  Persistence adapter that round-trips the frozen `MemoryProofToken` contract.
  """

  alias Mezzanine.Audit.{MemoryProofToken, MemoryProofTokenRecord}

  @contract_fields [
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
    :proof_hash,
    :trace_id,
    :parent_fragment_id,
    :child_fragment_id,
    :evidence_refs,
    :governance_decision_ref,
    :metadata
  ]

  @spec emit(MemoryProofToken.t() | map()) :: {:ok, MemoryProofToken.t()} | {:error, term()}
  def emit(%MemoryProofToken{} = token) do
    token
    |> Map.from_struct()
    |> emit()
  end

  def emit(attrs) when is_map(attrs) do
    with {:ok, token} <- MemoryProofToken.new(attrs),
         {:ok, %MemoryProofTokenRecord{} = record} <-
           MemoryProofTokenRecord.store(Map.from_struct(token)) do
      to_contract(record)
    end
  end

  @spec fetch(String.t()) :: {:ok, MemoryProofToken.t()} | {:error, term()}
  def fetch(proof_id) when is_binary(proof_id) do
    case MemoryProofTokenRecord.by_proof_id(proof_id) do
      {:ok, %MemoryProofTokenRecord{} = record} ->
        to_contract(record)

      {:error, error} ->
        {:error, error}
    end
  end

  @spec list_trace(String.t(), String.t()) :: {:ok, [MemoryProofToken.t()]} | {:error, term()}
  def list_trace(tenant_ref, trace_id) when is_binary(tenant_ref) and is_binary(trace_id) do
    with {:ok, records} <- MemoryProofTokenRecord.list_trace(tenant_ref, trace_id) do
      to_contracts(records, [])
    end
  end

  @spec verify_hash(MemoryProofToken.t() | map()) ::
          :ok | {:error, {:invalid_proof_hash, :proof_hash} | {:proof_hash_mismatch, map()}}
  def verify_hash(token), do: MemoryProofToken.verify_hash(token)

  defp to_contracts([], tokens), do: {:ok, Enum.reverse(tokens)}

  defp to_contracts([record | records], tokens) do
    case to_contract(record) do
      {:ok, token} -> to_contracts(records, [token | tokens])
      {:error, error} -> {:error, error}
    end
  end

  defp to_contract(%MemoryProofTokenRecord{} = record) do
    record
    |> Map.take(@contract_fields)
    |> MemoryProofToken.new()
  end
end
