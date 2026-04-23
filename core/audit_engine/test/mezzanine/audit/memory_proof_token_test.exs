defmodule Mezzanine.Audit.MemoryProofTokenTest do
  use Mezzanine.Audit.DataCase, async: false

  alias Mezzanine.Audit.{MemoryProofToken, MemoryProofTokenRecord, MemoryProofTokenStore}

  @event_time ~U[2026-04-23 08:00:00.000000Z]
  @kinds [:recall, :write_private, :share_up, :promote, :invalidate, :audit]

  test "emits and lists proof tokens for all six kinds by tenant trace" do
    for {kind, offset} <- Enum.with_index(@kinds) do
      assert {:ok, token} =
               valid_attrs(%{
                 proof_id: "proof-#{kind}",
                 kind: kind,
                 t_event: DateTime.add(@event_time, offset, :second)
               })
               |> MemoryProofTokenStore.emit()

      assert token.kind == kind
      assert token.trace_id == "trace-proof"
    end

    assert {:ok, records} = MemoryProofTokenRecord.list_trace("tenant-alpha", "trace-proof")
    assert Enum.map(records, & &1.kind) == @kinds

    assert has_index?("memory_proof_tokens", ["tenant_ref", "trace_id", "t_event"])
    assert has_index?("memory_proof_tokens", ["tenant_ref", "proof_hash"])
  end

  test "zero fragment recall still emits and hash verification succeeds" do
    assert {:ok, token} =
             valid_attrs(%{
               proof_id: "proof-zero-fragment",
               kind: :recall,
               fragment_ids: [],
               transform_hashes: [],
               access_projection_hashes: []
             })
             |> MemoryProofTokenStore.emit()

    assert token.kind == :recall
    assert token.fragment_ids == []
    assert :ok = MemoryProofToken.verify_hash(token)
  end

  test "uses AITrace process context when trace_id is omitted" do
    Process.put(:aitrace_context, %{trace_id: "trace-from-aitrace"})
    on_exit(fn -> Process.delete(:aitrace_context) end)

    assert {:ok, token} =
             valid_attrs(%{proof_id: "proof-aitrace-context"})
             |> Map.delete(:trace_id)
             |> MemoryProofTokenStore.emit()

    assert token.trace_id == "trace-from-aitrace"
  end

  test "rejects missing policy refs" do
    assert {:error, {:missing_proof_token_fields, fields}} =
             valid_attrs(%{
               proof_id: "proof-missing-policy",
               policy_refs: []
             })
             |> MemoryProofTokenStore.emit()

    assert :policy_refs in fields
  end

  test "rejects empty proof hash and detects tampering" do
    assert {:error, {:invalid_proof_hash, :proof_hash}} =
             valid_attrs(%{
               proof_id: "proof-empty-hash",
               proof_hash: ""
             })
             |> MemoryProofToken.new()

    assert {:ok, token} =
             valid_attrs(%{
               proof_id: "proof-tamper-check"
             })
             |> MemoryProofTokenStore.emit()

    tampered = %{token | proof_hash: String.duplicate("0", 64)}

    assert {:error, {:proof_hash_mismatch, _details}} =
             MemoryProofToken.verify_hash(tampered)
  end

  defp has_index?(table_name, columns) when is_binary(table_name) and is_list(columns) do
    columns_sql = Enum.join(columns, ", ")

    Repo.query!(
      """
      SELECT indexdef
      FROM pg_indexes
      WHERE schemaname = current_schema()
        AND tablename = $1
      """,
      [table_name]
    ).rows
    |> Enum.any?(fn [indexdef] ->
      String.contains?(indexdef, "(#{columns_sql})")
    end)
  end

  defp valid_attrs(overrides) do
    %{
      proof_id: "proof-default",
      kind: :recall,
      tenant_ref: "tenant-alpha",
      installation_id: "installation-alpha",
      subject_id: "subject-alpha",
      execution_id: "execution-alpha",
      user_ref: "user-alpha",
      agent_ref: "agent-alpha",
      t_event: @event_time,
      epoch_used: 42,
      policy_refs: [%{id: "policy-read", version: 1}],
      fragment_ids: ["fragment-a", "fragment-b"],
      transform_hashes: [String.duplicate("a", 64)],
      access_projection_hashes: [String.duplicate("b", 64)],
      trace_id: "trace-proof",
      metadata: %{
        "operation_ref" => "memory-proof-token-test",
        "status" => "admitted"
      }
    }
    |> Map.merge(overrides)
  end
end
