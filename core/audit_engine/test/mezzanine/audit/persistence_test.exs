defmodule Mezzanine.Audit.PersistenceTest do
  use Mezzanine.Audit.DataCase, async: false

  alias Mezzanine.Audit.{
    AuditAppend,
    AuditFact,
    AuditInclusionProof,
    AuditQuery,
    ExecutionLineage,
    ExecutionLineageStore
  }

  test "record persists audit facts with operator trace joins" do
    occurred_at = ~U[2026-04-16 01:00:00.000000Z]

    assert {:ok, fact} =
             AuditFact.record(%{
               installation_id: "inst-1",
               subject_id: "subject-1",
               execution_id: "exec-1",
               fact_kind: :execution_completed,
               actor_ref: %{kind: :system},
               payload: %{result_summary: %{status: "ok"}},
               trace_id: "trace-1",
               causation_id: "cause-1",
               occurred_at: occurred_at
             })

    assert fact.trace_id == "trace-1"
    assert fact.causation_id == "cause-1"
    assert fact.fact_kind == :execution_completed

    assert {:ok, [reloaded]} = AuditFact.list_trace("inst-1", "trace-1")
    assert reloaded.id == fact.id
    assert reloaded.occurred_at == occurred_at

    assert has_index?("audit_facts", ["installation_id", "trace_id", "occurred_at"])
    assert has_index?("audit_facts", ["causation_id"])
  end

  test "audit-owned append command is idempotent by installation and idempotency key" do
    occurred_at = ~U[2026-04-20 19:30:00.000000Z]

    attrs = %{
      installation_id: "inst-append",
      subject_id: "subject-append",
      execution_id: "exec-append",
      fact_kind: :execution_completed,
      actor_ref: %{kind: :system},
      payload: %{result_summary: %{status: "ok"}},
      trace_id: "trace-append",
      causation_id: "cause-append",
      occurred_at: occurred_at,
      idempotency_key: "audit-append:test:exec-append"
    }

    assert {:ok, first} = AuditAppend.append_fact(attrs)

    assert {:ok, second} =
             attrs
             |> put_in([:payload, :result_summary, :status], "changed")
             |> AuditAppend.append_fact()

    assert second.audit_fact_id == first.audit_fact_id
    assert second.idempotency_key == first.idempotency_key

    assert {:ok, [reloaded]} = AuditFact.list_trace("inst-append", "trace-append")
    assert reloaded.id == first.audit_fact_id
    assert reloaded.payload["result_summary"]["status"] == "ok"
    assert reloaded.idempotency_key == "audit-append:test:exec-append"

    assert reloaded.payload["audit_observability_counts"] ==
             expected_observability_counts(%{
               "admitted_count" => 2,
               "deduped_count" => 1
             })

    assert has_index?("audit_facts", ["installation_id", "idempotency_key"])
  end

  test "failure audit append requires amplification guard evidence" do
    attrs = failure_audit_attrs()

    assert {:error, {:missing_audit_amplification_guard, required_fields}} =
             AuditAppend.append_fact(attrs)

    assert "admission_key" in required_fields
    assert "suppressed_count" in required_fields
    assert "overflow_counter_ref" in required_fields
    assert "unavailable_guard_safe_action" in required_fields
  end

  test "audit amplification guard declares admission window and aggregate counters" do
    attrs = failure_audit_attrs()
    idempotency_key = AuditAppend.idempotency_key(attrs)

    guarded_attrs =
      attrs
      |> Map.put(:idempotency_key, idempotency_key)
      |> AuditAppend.put_amplification_guard()

    guard = guarded_attrs.payload["audit_amplification_guard"]

    assert guard["window_ms"] == 60_000
    assert guard["window_started_at"] == "2026-04-20T19:45:00.000Z"
    assert guard["max_events_per_key_per_window"] == 1
    assert guard["aggregate_counter_ref"] == "mezzanine.audit_repeat_aggregation.v1"
    assert guard["suppressed_count"] == 0
    assert guard["overflow_counter_ref"] == "mezzanine.audit_overflow.count"
    assert guard["unavailable_guard_safe_action"] == "reject_audit_append"

    assert guard["admission_key"] == %{
             "tenant_or_partition" => "inst-failure",
             "owner_package" => "core/audit_engine",
             "source_boundary" => "Mezzanine.Audit.AuditAppend",
             "event_name" => "execution_failed",
             "error_class" => "semantic_failure",
             "safe_action" => "aggregate_repeated_audit_fact",
             "canonical_idempotency_key_or_payload_hash" => idempotency_key
           }

    assert {:ok, result} = AuditAppend.append_fact(guarded_attrs)
    assert String.starts_with?(result.idempotency_key, "audit-aggregate:")
    refute result.idempotency_key == idempotency_key

    assert {:ok, [reloaded]} = AuditFact.list_trace("inst-failure", "trace-failure")
    assert reloaded.payload["audit_amplification_guard"] == guard
    assert reloaded.idempotency_key == result.idempotency_key

    assert reloaded.payload["audit_observability_counts"] ==
             expected_observability_counts(%{
               "admitted_count" => 1,
               "hashed_count" => 1
             })
  end

  test "repeated same-key failure audit appends aggregate inside declared window" do
    attrs = failure_audit_attrs()
    idempotency_key = AuditAppend.idempotency_key(attrs)

    first_attrs =
      attrs
      |> Map.put(:idempotency_key, idempotency_key)
      |> AuditAppend.put_amplification_guard()

    repeated_attrs =
      attrs
      |> Map.put(:occurred_at, DateTime.add(attrs.occurred_at, 30, :second))
      |> Map.put(:idempotency_key, idempotency_key)
      |> AuditAppend.put_amplification_guard()

    next_window_attrs =
      attrs
      |> Map.put(:occurred_at, DateTime.add(attrs.occurred_at, 61, :second))
      |> Map.put(:idempotency_key, idempotency_key)
      |> AuditAppend.put_amplification_guard()

    assert {:ok, first} = AuditAppend.append_fact(first_attrs)
    assert {:ok, repeated} = AuditAppend.append_fact(repeated_attrs)

    assert repeated.audit_fact_id == first.audit_fact_id
    assert repeated.idempotency_key == first.idempotency_key

    assert {:ok, [aggregated]} = AuditFact.list_trace("inst-failure", "trace-failure")
    guard = aggregated.payload["audit_amplification_guard"]

    assert guard["first_seen_at"] ==
             first_attrs.payload["audit_amplification_guard"]["first_seen_at"]

    assert guard["last_seen_at"] ==
             repeated_attrs.payload["audit_amplification_guard"]["last_seen_at"]

    assert guard["suppressed_count"] == 1

    assert aggregated.payload["audit_aggregation"]["aggregate_counter_ref"] ==
             "mezzanine.audit_repeat_aggregation.v1"

    assert aggregated.payload["audit_aggregation"]["suppressed_count"] == 1

    assert aggregated.payload["audit_aggregation"]["safe_action"] ==
             "aggregate_repeated_audit_fact"

    assert aggregated.payload["audit_observability_counts"] ==
             expected_observability_counts(%{
               "admitted_count" => 2,
               "aggregated_count" => 1,
               "hashed_count" => 2
             })

    assert {:ok, next_window} = AuditAppend.append_fact(next_window_attrs)
    refute next_window.audit_fact_id == first.audit_fact_id

    assert {:ok, facts} = AuditFact.list_trace("inst-failure", "trace-failure")
    assert length(facts) == 2
  end

  test "audit-owned query returns decision terminal attempt facts" do
    assert {:ok, _fact} =
             AuditAppend.append_fact(%{
               installation_id: "inst-query",
               subject_id: "subject-query",
               execution_id: "exec-query",
               decision_id: "decision-query",
               fact_kind: :decision_terminal_resolution_attempt,
               actor_ref: %{kind: :system},
               payload: %{outcome: "accepted"},
               trace_id: "trace-query",
               causation_id: "cause-query",
               idempotency_key: "audit-query:test:decision-query"
             })

    assert {:ok, [attempt]} =
             AuditQuery.decision_terminal_resolution_attempts("inst-query", "decision-query")

    assert attempt.payload["outcome"] == "accepted"
    assert attempt.fact_kind == :decision_terminal_resolution_attempt
  end

  test "audit inclusion proof derives linear checkpoint evidence from a fact" do
    assert {:ok, _fact_ref} =
             AuditAppend.append_fact(%{
               installation_id: "inst-proof",
               subject_id: "subject-proof",
               execution_id: "exec-proof",
               fact_kind: :execution_completed,
               actor_ref: %{kind: :system},
               payload: %{status: "ok", artifact_refs: ["artifact-proof"]},
               trace_id: "trace-proof",
               causation_id: "cause-proof",
               idempotency_key: "audit-proof:test:exec-proof"
             })

    assert {:ok, [fact]} = AuditFact.list_trace("inst-proof", "trace-proof")

    assert {:ok, proof} =
             AuditInclusionProof.from_fact(fact, %{
               position: 7,
               checkpoint_ref: "audit-checkpoint:inst-proof:7",
               release_manifest_ref: "phase5-v7-hardening"
             })

    assert proof.proof_type == "linear_checkpoint"
    assert proof.audit_fact_id == fact.id
    assert proof.installation_id == fact.installation_id
    assert proof.trace_id == fact.trace_id
    assert proof.fact_kind == "execution_completed"
    assert proof.occurred_at == fact.occurred_at
    assert proof.position == 7
    assert proof.checkpoint_ref == "audit-checkpoint:inst-proof:7"
    assert proof.algorithm == AuditInclusionProof.default_algorithm()
    assert proof.release_manifest_ref == "phase5-v7-hardening"
    assert proof.fact_hash == AuditInclusionProof.fact_hash(fact)
    assert proof.payload_hash == AuditInclusionProof.payload_hash(fact.payload)
    assert proof.root_hash == nil
    assert proof.sibling_path == []
    assert AuditInclusionProof.linear_checkpoint?(proof)
    refute AuditInclusionProof.merkle_tree?(proof)
  end

  test "audit inclusion proof reserves merkle_tree for explicit root evidence" do
    fact_hash = String.duplicate("a", 64)
    sibling_path = [%{side: "left", hash: String.duplicate("d", 64)}]
    assert {:ok, root_hash} = AuditInclusionProof.merkle_root_hash(fact_hash, sibling_path)

    attrs = %{
      proof_type: "merkle_tree",
      audit_fact_id: "audit-fact-merkle",
      installation_id: "inst-merkle",
      trace_id: "trace-merkle",
      fact_kind: "execution_completed",
      occurred_at: ~U[2026-04-20 21:00:00.000000Z],
      fact_hash: fact_hash,
      payload_hash: String.duplicate("b", 64),
      sequence: 1,
      checkpoint_ref: "audit-checkpoint:inst-merkle:1",
      algorithm: AuditInclusionProof.default_algorithm(),
      release_manifest_ref: "phase5-v7-hardening"
    }

    assert {:error, {:invalid_hash, :root_hash}} = AuditInclusionProof.new(attrs)

    assert {:error, {:missing_sibling_path, :sibling_path}} =
             attrs
             |> Map.put(:root_hash, root_hash)
             |> AuditInclusionProof.new()

    assert {:error, {:merkle_root_mismatch, %{computed: ^root_hash}}} =
             attrs
             |> Map.put(:root_hash, String.duplicate("c", 64))
             |> Map.put(:sibling_path, sibling_path)
             |> AuditInclusionProof.new()

    assert {:error, {:invalid_sibling_side, "inside"}} =
             attrs
             |> Map.put(:root_hash, root_hash)
             |> Map.put(:sibling_path, [%{side: "inside", hash: String.duplicate("d", 64)}])
             |> AuditInclusionProof.new()

    assert {:ok, proof} =
             attrs
             |> Map.put(:root_hash, root_hash)
             |> Map.put(:sibling_path, sibling_path)
             |> AuditInclusionProof.new()

    assert proof.proof_type == "merkle_tree"
    assert proof.root_hash == root_hash
    assert proof.sibling_path == [%{"side" => "left", "hash" => String.duplicate("d", 64)}]
    assert proof.sequence == 1
    assert AuditInclusionProof.merkle_tree?(proof)
  end

  test "audit inclusion proof rejects missing common fields and missing position" do
    attrs = valid_inclusion_proof_attrs()

    assert AuditInclusionProof.required_fields() == [
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

    for field <- AuditInclusionProof.required_fields() do
      assert {:error, {:missing_inclusion_proof_fields, missing}} =
               attrs
               |> Map.delete(field)
               |> AuditInclusionProof.new()

      assert field in missing
    end

    assert {:error, {:missing_inclusion_position, [:position, :sequence]}} =
             attrs
             |> Map.delete(:position)
             |> AuditInclusionProof.new()
  end

  test "execution lineage store upserts stable bridge linkage by execution id" do
    initial_lineage =
      ExecutionLineage.new!(%{
        trace_id: "trace-1",
        causation_id: "cause-1",
        installation_id: "inst-1",
        subject_id: "subject-1",
        execution_id: "exec-1",
        citadel_submission_id: "citadel-sub-1",
        ji_submission_key: "ji-sub-1"
      })

    assert {:ok, stored_lineage} = ExecutionLineageStore.store(initial_lineage)
    assert stored_lineage.execution_id == "exec-1"

    updated_lineage =
      ExecutionLineage.new!(%{
        trace_id: "trace-1",
        causation_id: "cause-1",
        installation_id: "inst-1",
        subject_id: "subject-1",
        execution_id: "exec-1",
        citadel_submission_id: "citadel-sub-1",
        ji_submission_key: "ji-sub-1",
        lower_run_id: "run-1",
        lower_attempt_id: "attempt-1",
        artifact_refs: ["artifact-1"]
      })

    assert {:ok, persisted_lineage} = ExecutionLineageStore.store(updated_lineage)
    assert persisted_lineage.lower_run_id == "run-1"

    assert {:ok, fetched_lineage} = ExecutionLineageStore.fetch("exec-1")

    assert %{
             installation_id: "inst-1",
             subject_id: "subject-1",
             execution_id: "exec-1",
             trace_id: "trace-1"
           } == ExecutionLineage.public_lookup(fetched_lineage)

    assert %{
             citadel_submission_id: "citadel-sub-1",
             ji_submission_key: "ji-sub-1",
             lower_run_id: "run-1",
             lower_attempt_id: "attempt-1",
             artifact_refs: ["artifact-1"]
           } == ExecutionLineage.lower_identifiers(fetched_lineage)

    assert {:ok, [trace_lineage]} = ExecutionLineageStore.list_trace("inst-1", "trace-1")
    assert trace_lineage.execution_id == "exec-1"

    assert has_index?("execution_lineage_records", ["execution_id"])
    assert has_index?("execution_lineage_records", ["installation_id", "trace_id"])
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

  defp valid_inclusion_proof_attrs do
    %{
      proof_type: "linear_checkpoint",
      audit_fact_id: "audit-fact-fields",
      installation_id: "inst-fields",
      trace_id: "trace-fields",
      fact_kind: "execution_completed",
      occurred_at: ~U[2026-04-20 21:30:00.000000Z],
      fact_hash: String.duplicate("a", 64),
      payload_hash: String.duplicate("b", 64),
      position: 11,
      checkpoint_ref: "audit-checkpoint:inst-fields:11",
      algorithm: AuditInclusionProof.default_algorithm(),
      release_manifest_ref: "phase5-v7-hardening"
    }
  end

  defp expected_observability_counts(overrides) do
    %{
      "count_ref" => "mezzanine.audit_append.observability_counts.v1",
      "admitted_count" => 0,
      "deduped_count" => 0,
      "aggregated_count" => 0,
      "dropped_count" => 0,
      "truncated_count" => 0,
      "hashed_count" => 0,
      "spilled_count" => 0,
      "sampled_count" => 0,
      "rejected_count" => 0,
      "overflow_count" => 0
    }
    |> Map.merge(overrides)
  end

  defp failure_audit_attrs do
    %{
      installation_id: "inst-failure",
      subject_id: "subject-failure",
      execution_id: "exec-failure",
      fact_kind: :execution_failed,
      actor_ref: %{kind: :system},
      payload: %{classification: "semantic_failure", failure_kind: "semantic_failure"},
      trace_id: "trace-failure",
      causation_id: "cause-failure",
      occurred_at: ~U[2026-04-20 19:45:00.000000Z]
    }
  end
end
