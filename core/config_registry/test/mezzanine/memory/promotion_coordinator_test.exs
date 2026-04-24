defmodule Mezzanine.Memory.PromotionCoordinatorTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Audit.TemporalQueueRouting

  alias Mezzanine.Memory.{
    MemoryCandidate,
    MemoryPromotionDecision,
    PromotionCoordinator
  }

  @tenant_ref "tenant://alpha"
  @installation_ref "installation://alpha/prod/outer-brain"
  @node_ref "node://mez_1@127.0.0.1/node-a"
  @node_instance_id "12345678-90ab-cdef-1234-567890abcdef"
  @commit_hlc %{"w" => 1_800_000_000_000_000_000, "l" => 4, "n" => @node_ref}

  test "memory candidates require node and commit-order evidence plus evidence and governance" do
    assert {:ok, candidate} = MemoryCandidate.V1.new(candidate_attrs())
    assert candidate.source_node_ref == @node_ref
    assert candidate.commit_lsn == "16/B374D84B"
    assert candidate.commit_hlc == @commit_hlc
    assert candidate.evidence_refs != []
    assert candidate.governance_refs != []

    assert {:error, {:missing_ordering_evidence, :source_node_ref}} =
             candidate_attrs()
             |> Map.delete(:source_node_ref)
             |> MemoryCandidate.V1.new()

    assert {:error, {:missing_required_fields, [:evidence_refs]}} =
             candidate_attrs(evidence_refs: [])
             |> MemoryCandidate.V1.new()

    assert {:error, {:missing_required_fields, [:governance_refs]}} =
             candidate_attrs(governance_refs: [])
             |> MemoryCandidate.V1.new()
  end

  test "auto-approved promotion writes governed memory and emits attachment projection and proof" do
    assert {:ok, candidate} = MemoryCandidate.V1.new(candidate_attrs())
    test_pid = self()

    callbacks =
      callbacks(test_pid,
        promote_policy: fn context ->
          send(test_pid, {:promote_policy, context.candidate.candidate_id})
          {:ok, promote_policy(review_required: false, auto_decide: true)}
        end,
        auto_decide: fn context ->
          send(test_pid, {:auto_decide, context.candidate.candidate_id})
          {:ok, %{decision: :approve, reason: "score above threshold"}}
        end
      )

    candidate_id = candidate.candidate_id
    assert {:ok, ^candidate_id} = PromotionCoordinator.propose_candidate(candidate, callbacks)

    assert_received {:workflow_plan, plan}
    assert plan.workflow_type == "memory_promotion"
    assert plan.workflow_version == "memory-promotion.v1"
    assert plan.workflow_id =~ candidate.candidate_id
    assert plan.idempotency_key =~ candidate.candidate_id
    assert plan.idempotency_key =~ candidate.promotion_policy_ref
    assert plan.task_queue == TemporalQueueRouting.promotion_queue(@installation_ref)
    assert plan.worker_identity =~ ~r/\Amez-a\/12345678\/promotion_worker\/[a-z2-7]{20}\z/
    refute plan.worker_identity =~ @installation_ref
    refute plan.worker_identity =~ "PID"
    assert plan.signal_name == "memory.promotion.decision"
    assert plan.signal_version == "memory-promotion-decision.v1"

    assert_received {:governed_insert, governed_fragment, decision}

    assert %MemoryPromotionDecision.V1{decision: :approved, decision_source: :auto_decide} =
             decision

    assert governed_fragment.tier == :governed
    assert governed_fragment.parent_fragment_id == candidate.shared_fragment_id
    assert governed_fragment.promotion_decision_ref == decision.decision_id
    assert governed_fragment.promotion_policy_ref == candidate.promotion_policy_ref
    assert governed_fragment.source_node_ref == @node_ref

    assert_received {:derived_state_attachment, attachment, ^decision}
    assert attachment.subject.ref == governed_fragment.fragment_id
    assert attachment.evidence_refs == candidate.evidence_refs
    assert attachment.governance_refs == candidate.governance_refs

    assert_received {:projection_event, projection_event, ^decision}
    assert projection_event.downstream_only? == true
    assert projection_event.governed_truth_owner == :mezzanine
    assert projection_event.fragment_id == governed_fragment.fragment_id

    assert_received {:promotion_proof, proof_token, ^decision}
    assert proof_token.kind == :promote
    assert proof_token.source_node_ref == @node_ref
    assert proof_token.commit_lsn == candidate.commit_lsn
    assert proof_token.commit_hlc == @commit_hlc
    assert proof_token.governance_decision_ref["decision"] == "approved"
    assert proof_token.metadata["promotion_status"] == "approved"
  end

  test "review-gated promotion waits for quorum decision before governed write" do
    assert {:ok, candidate} = MemoryCandidate.V1.new(candidate_attrs())
    test_pid = self()

    callbacks =
      callbacks(test_pid,
        promote_policy: fn _context ->
          {:ok, promote_policy(review_required: true, auto_decide: false)}
        end,
        enqueue_review: fn context ->
          send(test_pid, {:enqueue_review, context.candidate.candidate_id})
          {:ok, %{review_ref: "review://memory/#{context.candidate.candidate_id}"}}
        end,
        await_review_decision: fn context ->
          send(test_pid, {:await_review_decision, context.review.review_ref})

          {:ok,
           %{
             decision: :approve,
             decision_ref: "review-decision://accept-1",
             review_refs: [context.review.review_ref]
           }}
        end
      )

    candidate_id = candidate.candidate_id
    assert {:ok, ^candidate_id} = PromotionCoordinator.propose_candidate(candidate, callbacks)

    assert_received {:enqueue_review, _candidate_id}
    assert_received {:await_review_decision, "review://memory/" <> _rest}
    assert_received {:governed_insert, _governed_fragment, decision}
    assert decision.decision_source == :review
    assert decision.review_refs != []
  end

  test "denied promotion emits proof and does not write governed memory" do
    assert {:ok, candidate} = MemoryCandidate.V1.new(candidate_attrs())
    test_pid = self()

    callbacks =
      callbacks(test_pid,
        promote_policy: fn _context ->
          {:ok, promote_policy(review_required: false, auto_decide: true)}
        end,
        auto_decide: fn _context ->
          {:ok, %{decision: :deny, reason: "insufficient evidence"}}
        end
      )

    candidate_id = candidate.candidate_id
    assert {:ok, ^candidate_id} = PromotionCoordinator.propose_candidate(candidate, callbacks)

    refute_received {:governed_insert, _fragment, _decision}
    assert_received {:promotion_proof, proof_token, decision}
    assert decision.decision == :denied
    assert proof_token.metadata["promotion_status"] == "denied"
    assert proof_token.child_fragment_id == nil
  end

  test "quarantined candidates fail closed before review or governed writes" do
    assert {:ok, candidate} = MemoryCandidate.V1.new(candidate_attrs(quarantined?: true))

    assert {:error, :quarantined_candidate} =
             PromotionCoordinator.propose_candidate(candidate, callbacks(self()))

    refute_received {:workflow_plan, _plan}
    refute_received {:governed_insert, _fragment, _decision}
  end

  test "unknown promotion workflow signal versions fail closed" do
    assert :ok =
             PromotionCoordinator.validate_workflow_signal(%{
               signal_name: "memory.promotion.decision",
               signal_version: "memory-promotion-decision.v1",
               candidate_id: "memory-candidate://1",
               decision_id: "promotion-decision://1"
             })

    assert {:error,
            {:unregistered_signal, "memory.promotion.decision", "memory-promotion-decision.v0"}} =
             PromotionCoordinator.validate_workflow_signal(%{
               signal_name: "memory.promotion.decision",
               signal_version: "memory-promotion-decision.v0",
               candidate_id: "memory-candidate://1",
               decision_id: "promotion-decision://1"
             })

    assert {:error, {:missing_required_fields, [:signal_version]}} =
             PromotionCoordinator.validate_workflow_signal(%{
               signal_name: "memory.promotion.decision",
               candidate_id: "memory-candidate://1",
               decision_id: "promotion-decision://1"
             })
  end

  test "duplicate candidates from different nodes fail closed before governed writes" do
    assert {:ok, candidate} = MemoryCandidate.V1.new(candidate_attrs())

    callbacks =
      callbacks(self(),
        claim_candidate: fn context ->
          {:ok,
           %{
             candidate_id: context.candidate.candidate_id,
             source_node_ref: "node://mez_2@127.0.0.1/node-b"
           }}
        end
      )

    candidate_id = candidate.candidate_id

    assert {:error,
            {:duplicate_candidate_from_different_node,
             %{candidate_id: ^candidate_id, existing_source_node_ref: _existing}}} =
             PromotionCoordinator.propose_candidate(candidate, callbacks)

    refute_received {:governed_insert, _fragment, _decision}
  end

  defp callbacks(test_pid, overrides \\ []) do
    base = [
      node_shortname: "mez-a",
      node_instance_id: @node_instance_id,
      claim_candidate: fn _context -> {:ok, :new} end,
      promote_policy: fn _context -> {:ok, promote_policy()} end,
      workflow_enqueue: fn plan ->
        send(test_pid, {:workflow_plan, plan})
        {:ok, %{workflow_ref: plan.workflow_id}}
      end,
      auto_decide: fn _context -> {:ok, %{decision: :approve, reason: "default approve"}} end,
      enqueue_review: fn context ->
        {:ok, %{review_ref: "review://memory/#{context.candidate.candidate_id}"}}
      end,
      await_review_decision: fn context ->
        {:ok, %{decision: :approve, review_refs: [context.review.review_ref]}}
      end,
      insert_governed: fn governed_fragment, context ->
        send(test_pid, {:governed_insert, governed_fragment, context.decision})
        {:ok, governed_fragment}
      end,
      emit_derived_state_attachment: fn attachment, context ->
        send(test_pid, {:derived_state_attachment, attachment, context.decision})

        {:ok,
         Map.put(
           attachment,
           :attachment_ref,
           "derived-state://memory/#{context.candidate.candidate_id}"
         )}
      end,
      emit_projection_event: fn event, context ->
        send(test_pid, {:projection_event, event, context.decision})
        :ok
      end,
      emit_proof: fn proof_token, context ->
        send(test_pid, {:promotion_proof, proof_token, context.decision})
        {:ok, proof_token}
      end
    ]

    Keyword.merge(base, overrides)
  end

  defp candidate_attrs(overrides \\ []) do
    %{
      tenant_ref: @tenant_ref,
      installation_ref: @installation_ref,
      shared_fragment_id: "memory-shared://alpha/shared-1",
      source_fragment_ids: ["memory-private://alpha/private-1"],
      source_node_ref: @node_ref,
      commit_lsn: "16/B374D84B",
      commit_hlc: @commit_hlc,
      t_epoch: 44,
      trace_id: "trace-promotion-1",
      promotion_policy_ref: "promote-policy://alpha/strict",
      source_agents: ["agent://writer"],
      source_resources: ["resource://doc/1"],
      source_scopes: ["scope://private/user-a"],
      access_agents: ["agent://governed-reader"],
      access_resources: ["resource://doc/1"],
      access_scopes: ["scope://installation/alpha"],
      access_projection_hash: "sha256:projection",
      applied_policies: ["share-up-policy://team-alpha", "promote-policy://alpha/strict"],
      evidence_refs: [%{ref: "evidence://review-packet/1", kind: "review_packet"}],
      governance_refs: [%{ref: "governance://memory/promotion", kind: "promotion"}],
      content_hash: "sha256:content",
      content_ref: %{uri: "memory-content://shared-1"},
      schema_ref: "schema://memory/shared",
      rebuild_spec: %{source: "shared", transform: "governed-summary"},
      metadata: %{source: "test"}
    }
    |> Map.merge(Map.new(overrides))
  end

  defp promote_policy(overrides \\ []) do
    %{
      policy_id: "promote-policy://alpha/strict",
      version: 7,
      granularity_scope: :installation,
      review_required: true,
      quorum_ref: "quorum://memory-review/default",
      auto_decide: false,
      evidence_requirements: [%{kind: "review_packet"}],
      audit_level: :strict
    }
    |> Map.merge(Map.new(overrides))
  end
end
