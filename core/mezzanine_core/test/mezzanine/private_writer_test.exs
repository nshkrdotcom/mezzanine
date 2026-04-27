defmodule Mezzanine.PrivateWriterTest do
  use ExUnit.Case, async: false

  alias Mezzanine.PrivateWriter
  alias Mezzanine.PrivateWriter.{CommitReceipt, CommitRequest, M7AProof}

  setup do
    PrivateWriter.reset!()
    :ok
  end

  test "commits private memory through M2 caller with deterministic m7a proof and recall refs" do
    assert {:ok, %CommitRequest{} = request} = CommitRequest.new(commit_request_attrs())

    assert {:ok, %CommitReceipt{} = receipt} =
             PrivateWriter.commit(request, caller: :m2_agent_loop)

    assert receipt.private_commit.memory_commit_ref == request.memory_commit_ref
    assert receipt.private_commit.m7a_proof_refs == [receipt.m7a_proof.proof_ref]
    assert receipt.private_commit.recall_refs == PrivateWriter.recall_refs(receipt)
    assert receipt.private_commit.commit_lsn =~ "private-lsn://"
    assert receipt.private_commit.commit_hlc =~ "private-hlc://"

    assert %M7AProof{} = receipt.m7a_proof
    assert receipt.m7a_proof.integrity_hash =~ "sha256:"

    assert {:ok, duplicate} = PrivateWriter.commit(request, caller: :m2_agent_loop)
    assert duplicate == receipt
  end

  test "rejects missing proof inputs, raw payloads, and non-M2 callers" do
    assert {:error, :invalid_private_commit_request} =
             commit_request_attrs()
             |> Map.put(:candidate_fact_refs, [])
             |> CommitRequest.new()

    assert {:error, :invalid_private_commit_request} =
             commit_request_attrs()
             |> Map.delete(:authority_decision_ref)
             |> CommitRequest.new()

    assert {:error, :invalid_private_commit_request} =
             commit_request_attrs()
             |> put_in([:candidate_facts], [
               %{candidate_fact_ref: "candidate-fact://raw", raw_provider_payload: %{}}
             ])
             |> CommitRequest.new()

    assert {:error, :private_writer_requires_m2_caller} =
             PrivateWriter.commit(commit_request_attrs(), caller: :outer_brain)
  end

  test "rejects duplicate memory commit refs with divergent content" do
    assert {:ok, receipt} = PrivateWriter.commit(commit_request_attrs(), caller: :m2_agent_loop)

    divergent =
      commit_request_attrs()
      |> Map.put(:tenant_ref, "tenant://other")

    assert {:error, :divergent_private_commit} =
             PrivateWriter.commit(divergent, caller: :m2_agent_loop)

    assert receipt.private_commit.tenant_ref == "tenant://local"
  end

  test "integrity hash excludes retry and worker process noise by construction" do
    base = M7AProof.build(CommitRequest.new!(commit_request_attrs()))

    noisy =
      commit_request_attrs()
      |> Map.put(:worker_pid, inspect(self()))
      |> Map.put(:retry_attempt, 7)

    assert {:error, :invalid_private_commit_request} = CommitRequest.new(noisy)

    assert base.integrity_hash ==
             M7AProof.build(CommitRequest.new!(commit_request_attrs())).integrity_hash
  end

  defp commit_request_attrs do
    %{
      memory_commit_ref: "memory-commit://agent-loop/run-local-1/turn-1",
      tenant_ref: "tenant://local",
      subject_ref: "subject://task/1",
      run_ref: "run://local/1",
      turn_ref: "turn://local/1/1",
      candidate_fact_refs: ["candidate-fact://agent-loop/turn-1/1"],
      candidate_facts: [
        %{
          candidate_fact_ref: "candidate-fact://agent-loop/turn-1/1",
          fact_kind: :tool_observation,
          confidence_class: :observed,
          confidence_band: :high,
          risk_band: :low,
          source_observation_ref: "action-receipt://turn-1",
          evidence_ref: "evidence://turn-1",
          redaction_ref: "redaction://agent-loop/turn-1",
          redaction_class: :claim_checked,
          claim_check_refs: ["claim-check://agent-loop/turn-1/output"],
          proposed_by: "outer-brain://semanticize",
          trace_id: "trace://local/1"
        }
      ],
      source_observation_refs: ["action-receipt://turn-1"],
      authority_decision_ref: "authority-decision://turn-1",
      redaction_ref: "redaction://agent-loop/turn-1",
      redaction_class: :claim_checked,
      claim_check_refs: ["claim-check://agent-loop/turn-1/output"],
      idempotency_key: "agent-run:memory:turn-1:candidate-hash",
      trace_id: "trace://local/1",
      release_manifest_ref: "release-manifest://local/1"
    }
  end
end
