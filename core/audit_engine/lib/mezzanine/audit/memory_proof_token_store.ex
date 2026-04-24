defmodule Mezzanine.Audit.MemoryProofTokenStore do
  @moduledoc """
  Persistence adapter that round-trips the frozen `MemoryProofToken` contract.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Audit.{MemoryProofToken, MemoryProofTokenRecord}
  alias Mezzanine.Audit.Repo

  @contract_fields [
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
    :policy_refs,
    :fragment_ids,
    :transform_hashes,
    :access_projection_hashes,
    :proof_hash,
    :trace_id,
    :source_node_ref,
    :commit_lsn,
    :commit_hlc,
    :parent_fragment_id,
    :child_fragment_id,
    :evidence_refs,
    :governance_decision_ref,
    :metadata
  ]
  @write_family_kinds [:write_private, :share_up, :promote, :invalidate, :audit]
  @recall_idempotency_sql """
  SELECT proof_id
  FROM memory_proof_tokens
  WHERE kind = 'recall'
    AND trace_id = $1
    AND tenant_ref = $2
    AND epoch_used = $3
    AND user_ref = $4
    AND agent_ref = $5
  ORDER BY t_event ASC, proof_id ASC
  LIMIT 1
  """

  @spec emit(MemoryProofToken.t() | map()) :: {:ok, MemoryProofToken.t()} | {:error, term()}
  def emit(%MemoryProofToken{} = token) do
    emit_token(token)
  end

  def emit(attrs) when is_map(attrs) do
    with {:ok, token} <- MemoryProofToken.new(attrs) do
      emit_token(token)
    end
  end

  @spec emit_write_family(MemoryProofToken.t() | map(), (module() ->
                                                           {:ok, term()} | {:error, term()})) ::
          {:ok, %{result: term(), proof_token: MemoryProofToken.t()}} | {:error, term()}
  def emit_write_family(attrs, operation_fun) when is_function(operation_fun, 1) do
    with {:ok, token} <- MemoryProofToken.new(attrs),
         :ok <- validate_write_family_kind(token.kind) do
      token
      |> run_write_family_transaction(operation_fun)
      |> unwrap_transaction()
    end
  end

  @spec verify_trace(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def verify_trace(tenant_ref, trace_id) when is_binary(tenant_ref) and is_binary(trace_id) do
    with {:ok, tokens} <- list_trace(tenant_ref, trace_id) do
      verify_tokens(tokens, [])
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

  defp run_write_family_transaction(token, operation_fun) do
    Repo.transaction(fn -> emit_write_family_transaction(token, operation_fun) end)
  end

  defp unwrap_transaction({:ok, result}), do: {:ok, result}
  defp unwrap_transaction({:error, reason}), do: {:error, reason}

  defp emit_write_family_transaction(token, operation_fun) do
    case operation_fun.(Repo) do
      {:ok, result} ->
        case emit(token) do
          {:ok, proof_token} -> %{result: result, proof_token: proof_token}
          {:error, reason} -> Repo.rollback(reason)
        end

      {:error, reason} ->
        Repo.rollback(reason)

      other ->
        Repo.rollback({:invalid_write_family_result, other})
    end
  end

  defp emit_token(%MemoryProofToken{kind: :recall} = token) do
    case find_recall_duplicate(token) do
      {:ok, existing_token} -> {:ok, existing_token}
      :not_found -> store_token(token)
      {:error, reason} -> {:error, reason}
    end
  end

  defp emit_token(%MemoryProofToken{} = token), do: store_token(token)

  defp store_token(%MemoryProofToken{} = token) do
    case MemoryProofTokenRecord.store(Map.from_struct(token), return_notifications?: true) do
      {:ok, %MemoryProofTokenRecord{} = record} ->
        to_contract(record)

      {:ok, %MemoryProofTokenRecord{} = record, _notifications} ->
        to_contract(record)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp find_recall_duplicate(%MemoryProofToken{} = token) do
    params = [
      token.trace_id,
      token.tenant_ref,
      token.epoch_used,
      token.user_ref,
      token.agent_ref
    ]

    case SQL.query(Repo, @recall_idempotency_sql, params) do
      {:ok, %{rows: [[proof_id]]}} -> fetch(proof_id)
      {:ok, %{rows: []}} -> :not_found
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_write_family_kind(kind) when kind in @write_family_kinds, do: :ok
  defp validate_write_family_kind(kind), do: {:error, {:invalid_write_family_kind, kind}}

  defp verify_tokens([], proof_ids) do
    {:ok, %{verified_count: length(proof_ids), proof_ids: Enum.reverse(proof_ids)}}
  end

  defp verify_tokens([token | tokens], proof_ids) do
    case MemoryProofToken.verify_hash(token) do
      :ok -> verify_tokens(tokens, [token.proof_id | proof_ids])
      {:error, reason} -> {:error, reason}
    end
  end

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
