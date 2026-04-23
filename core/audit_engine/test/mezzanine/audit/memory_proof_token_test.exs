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
                 t_event: DateTime.add(@event_time, offset, :second),
                 proof_hash_version: "m7a.v1"
               })
               |> MemoryProofTokenStore.emit()

      assert token.kind == kind
      assert token.trace_id == "trace-proof"
      assert token.proof_hash_version == "m7a.v1"
      assert token.source_node_ref == "node://mez_a@127.0.0.1/node-a"
      assert token.commit_lsn == "16/B374D848"

      assert token.commit_hlc == %{
               "w" => 1_776_947_200_000_000_000,
               "l" => 0,
               "n" => "node://mez_a@127.0.0.1/node-a"
             }
    end

    assert {:ok, records} = MemoryProofTokenRecord.list_trace("tenant-alpha", "trace-proof")
    assert Enum.map(records, & &1.kind) == @kinds
    assert Enum.all?(records, &(&1.source_node_ref == "node://mez_a@127.0.0.1/node-a"))

    assert has_index?("memory_proof_tokens", ["tenant_ref", "trace_id", "t_event"])
    assert has_index?("memory_proof_tokens", ["tenant_ref", "proof_hash"])

    assert has_index?("memory_proof_tokens", [
             "trace_id",
             "tenant_ref",
             "epoch_used",
             "user_ref",
             "agent_ref"
           ])
  end

  test "zero fragment recall still emits and hash verification succeeds" do
    assert {:ok, token} =
             valid_attrs(%{
               proof_id: "proof-zero-fragment",
               kind: :recall,
               fragment_ids: [],
               transform_hashes: [],
               access_projection_hashes: [],
               proof_hash_version: "m7a.v1"
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
             valid_attrs(%{proof_id: "proof-aitrace-context", proof_hash_version: "m7a.v1"})
             |> Map.delete(:trace_id)
             |> MemoryProofTokenStore.emit()

    assert token.trace_id == "trace-from-aitrace"
  end

  test "rejects missing policy refs" do
    assert {:error, {:missing_proof_token_fields, fields}} =
             valid_attrs(%{
               proof_id: "proof-missing-policy",
               policy_refs: [],
               proof_hash_version: "m7a.v1"
             })
             |> MemoryProofTokenStore.emit()

    assert :policy_refs in fields
  end

  test "rejects empty proof hash and detects tampering" do
    assert {:error, {:invalid_proof_hash, :proof_hash}} =
             valid_attrs(%{
               proof_id: "proof-empty-hash",
               proof_hash_version: "m7a.v1",
               proof_hash: ""
             })
             |> MemoryProofToken.new()

    assert {:ok, token} =
             valid_attrs(%{
               proof_id: "proof-tamper-check",
               proof_hash_version: "m7a.v1"
             })
             |> MemoryProofTokenStore.emit()

    tampered = %{token | proof_hash: String.duplicate("0", 64)}

    assert {:error, {:proof_hash_mismatch, _details}} =
             MemoryProofToken.verify_hash(tampered)
  end

  test "verifies m6 and m7a hash versions under the same trace audit" do
    assert {:ok, m6_token} =
             valid_attrs(%{
               proof_id: "proof-m6-version",
               proof_hash_version: "m6.v1"
             })
             |> Map.drop([:source_node_ref, :commit_lsn, :commit_hlc])
             |> MemoryProofTokenStore.emit()

    assert {:ok, m7a_token} =
             valid_attrs(%{
               proof_id: "proof-m7a-version",
               proof_hash_version: "m7a.v1",
               kind: :audit,
               t_event: DateTime.add(@event_time, 1, :second)
             })
             |> MemoryProofTokenStore.emit()

    assert m6_token.proof_hash_version == "m6.v1"
    assert m6_token.source_node_ref == nil
    assert m7a_token.proof_hash_version == "m7a.v1"
    assert m7a_token.source_node_ref == "node://mez_a@127.0.0.1/node-a"
    assert m6_token.proof_hash != m7a_token.proof_hash

    assert {:ok, %{verified_count: 2, proof_ids: ["proof-m6-version", "proof-m7a-version"]}} =
             MemoryProofTokenStore.verify_trace("tenant-alpha", "trace-proof")
  end

  test "rejects tokens whose ordering fields do not match the declared hash version" do
    assert {:error, {:missing_proof_token_fields, fields}} =
             valid_attrs(%{
               proof_id: "proof-m7a-missing-ordering",
               proof_hash_version: "m7a.v1"
             })
             |> Map.delete(:commit_lsn)
             |> MemoryProofTokenStore.emit()

    assert :commit_lsn in fields

    assert {:error, {:version_field_mismatch, "m6.v1", fields}} =
             valid_attrs(%{
               proof_id: "proof-m6-with-ordering",
               proof_hash_version: "m6.v1"
             })
             |> MemoryProofTokenStore.emit()

    assert Enum.sort(fields) == [:commit_hlc, :commit_lsn, :source_node_ref]
  end

  test "recall proof emission is idempotent by trace tenant epoch user and agent" do
    assert {:ok, first} =
             valid_attrs(%{
               proof_id: "proof-recall-idempotent-a",
               proof_hash_version: "m7a.v1",
               kind: :recall
             })
             |> MemoryProofTokenStore.emit()

    assert {:ok, second} =
             valid_attrs(%{
               proof_id: "proof-recall-idempotent-b",
               proof_hash_version: "m7a.v1",
               kind: :recall
             })
             |> MemoryProofTokenStore.emit()

    assert second.proof_id == first.proof_id
    assert second.proof_hash == first.proof_hash
  end

  test "write-family proof emission shares the durable row transaction and rolls back on crash" do
    Repo.query!("CREATE TEMP TABLE IF NOT EXISTS proof_token_tx_probe (id text PRIMARY KEY)")
    Repo.query!("TRUNCATE proof_token_tx_probe")

    assert {:ok, %{result: "row-ok", proof_token: token}} =
             MemoryProofTokenStore.emit_write_family(
               valid_attrs(%{
                 proof_id: "proof-write-family-ok",
                 proof_hash_version: "m7a.v1",
                 kind: :write_private
               }),
               fn repo ->
                 repo.query!("INSERT INTO proof_token_tx_probe (id) VALUES ('row-ok')")
                 {:ok, "row-ok"}
               end
             )

    assert token.proof_id == "proof-write-family-ok"
    assert %{rows: [[1]]} = Repo.query!("SELECT count(*) FROM proof_token_tx_probe")

    assert {:error, :injected_crash_after_write} =
             MemoryProofTokenStore.emit_write_family(
               valid_attrs(%{
                 proof_id: "proof-write-family-crash",
                 proof_hash_version: "m7a.v1",
                 kind: :write_private
               }),
               fn repo ->
                 repo.query!("INSERT INTO proof_token_tx_probe (id) VALUES ('row-crash')")
                 {:error, :injected_crash_after_write}
               end
             )

    assert %{rows: [[nil]]} =
             Repo.query!("SELECT max(id) FROM proof_token_tx_probe WHERE id = 'row-crash'")

    assert {:error, _reason} = MemoryProofTokenStore.fetch("proof-write-family-crash")
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
      proof_hash_version: "m6.v1",
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
      source_node_ref: "node://mez_a@127.0.0.1/node-a",
      commit_lsn: "16/B374D848",
      commit_hlc: %{
        "w" => 1_776_947_200_000_000_000,
        "l" => 0,
        "n" => "node://mez_a@127.0.0.1/node-a"
      },
      trace_id: "trace-proof",
      metadata: %{
        "operation_ref" => "memory-proof-token-test",
        "status" => "admitted"
      }
    }
    |> Map.merge(overrides)
  end
end
