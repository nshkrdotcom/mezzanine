defmodule Mezzanine.Memory.InvalidationCoordinatorTest do
  use ExUnit.Case, async: true

  alias Mezzanine.ConfigRegistry.ClusterInvalidation
  alias Mezzanine.Memory.InvalidationCoordinator

  @tenant_ref "tenant://alpha"
  @installation_ref "installation://alpha/prod/outer-brain"
  @node_ref "node://mez_1@127.0.0.1/node-a"
  @commit_hlc %{"w" => 1_800_000_000_000_000_000, "l" => 7, "n" => @node_ref}
  @all_reasons [
    :user_deletion,
    :source_correction,
    :source_deletion,
    :policy_change,
    :tenant_offboarding,
    :operator_suppression,
    :semantic_quarantine,
    :retention_expiry
  ]

  test "invalidates every supported reason with durable rows, cache fanout, and proof evidence" do
    for reason <- @all_reasons do
      request = request(reason: reason, root_fragment_id: "fragment://private/root")
      test_pid = self()

      assert {:ok, result} =
               InvalidationCoordinator.invalidate(
                 request,
                 callbacks(test_pid, fragments: [fragment("fragment://private/root", :private)])
               )

      assert [%{reason: reason_string, fragment_id: "fragment://private/root"}] =
               result.invalidations

      assert reason_string == Atom.to_string(reason)
      assert result.proof_token.kind == :invalidate
      assert result.proof_token.source_node_ref == @node_ref
      assert result.proof_token.commit_lsn == "16/B374D84C"
      assert result.proof_token.commit_hlc == @commit_hlc

      assert_receive {:cache_invalidated, [%{fragment_id: "fragment://private/root"}], ^reason}
      assert_receive {:proof, proof_token, ^reason}
      assert proof_token.fragment_ids == ["fragment://private/root"]

      assert_received {:cluster_publish, %{topic: fragment_topic, metadata: fragment_metadata},
                       ^reason}

      assert fragment_topic ==
               ClusterInvalidation.fragment_topic!(@tenant_ref, "fragment://private/root")

      assert fragment_metadata["tenant_ref"] == @tenant_ref
      assert fragment_metadata["fragment_id"] == "fragment://private/root"
      assert fragment_metadata["parent_chain"] == []
      assert is_binary(fragment_metadata["invalidation_id"])

      assert_received {:cluster_publish, %{topic: durable_topic}, ^reason}
      assert durable_topic =~ "memory.invalidation."

      if reason in [:user_deletion, :tenant_offboarding] do
        assert_received {:access_edges_revoked, ^reason, @tenant_ref, @node_ref}
      else
        refute_received {:access_edges_revoked, ^reason, _tenant_ref, _node_ref}
      end
    end
  end

  test "cascades to descendants whose parent chain contains the invalidated fragment" do
    request = request(reason: :source_deletion, root_fragment_id: "fragment://private/root")
    test_pid = self()

    fragments = [
      fragment("fragment://private/root", :private),
      fragment("fragment://shared/child", :shared, parent_fragment_id: "fragment://private/root"),
      fragment("fragment://governed/grandchild", :governed,
        parent_fragment_id: "fragment://shared/child"
      ),
      fragment("fragment://shared/unrelated", :shared,
        parent_fragment_id: "fragment://private/other"
      )
    ]

    assert {:ok, result} =
             InvalidationCoordinator.invalidate(
               request,
               callbacks(test_pid, fragments: fragments)
             )

    assert Enum.map(result.invalidations, & &1.fragment_id) == [
             "fragment://private/root",
             "fragment://shared/child",
             "fragment://governed/grandchild"
           ]

    assert Enum.map(result.invalidations, & &1.parent_chain) == [
             [],
             ["fragment://private/root"],
             ["fragment://private/root", "fragment://shared/child"]
           ]

    refute Enum.any?(result.invalidations, &(&1.fragment_id == "fragment://shared/unrelated"))

    assert_receive {:cache_invalidated, invalidations, :source_deletion}
    refute Enum.any?(invalidations, &(&1.fragment_id == "fragment://shared/unrelated"))
  end

  test "policy-change invalidation publishes a policy topic with version and effective time" do
    test_pid = self()

    assert {:ok, _result} =
             InvalidationCoordinator.invalidate(
               request(
                 reason: :policy_change,
                 root_fragment_id: "fragment://governed/policy",
                 policy_ref: %{
                   policy_id: "policy://invalidate/default",
                   kind: :invalidate,
                   version: 3,
                   installation_ref: @installation_ref,
                   effective_at: ~U[2026-04-24 17:00:00Z]
                 }
               ),
               callbacks(test_pid, fragments: [fragment("fragment://governed/policy", :governed)])
             )

    assert_receive {:cluster_publish, %{topic: policy_topic, metadata: metadata}, :policy_change}
    assert policy_topic =~ "memory.policy."
    assert metadata["policy_id"] == "policy://invalidate/default"
    assert metadata["policy_version"] == 3
    assert metadata["effective_at"] == "2026-04-24T17:00:00Z"
  end

  test "fails closed without source node, commit order, evidence, or known reason" do
    assert {:error, {:missing_ordering_evidence, :source_node_ref}} =
             request()
             |> Map.delete(:source_node_ref)
             |> InvalidationCoordinator.invalidate(callbacks(self()))

    assert {:error, {:missing_ordering_evidence, :commit_lsn}} =
             request()
             |> Map.delete(:commit_lsn)
             |> InvalidationCoordinator.invalidate(callbacks(self()))

    assert {:error, {:missing_required_fields, [:evidence_refs]}} =
             request(evidence_refs: [])
             |> InvalidationCoordinator.invalidate(callbacks(self()))

    assert {:error, {:unsupported_invalidation_reason, "unknown_reason"}} =
             request(reason: "unknown_reason")
             |> InvalidationCoordinator.invalidate(callbacks(self()))
  end

  test "partitioned cache consumers fail closed before proof emission" do
    test_pid = self()

    callbacks =
      callbacks(test_pid,
        invalidate_caches: fn _rows, _context ->
          {:error, :cache_consumer_partitioned}
        end
      )

    assert {:error, :cache_consumer_partitioned} =
             InvalidationCoordinator.invalidate(request(), callbacks)

    refute_received {:proof, _proof, _reason}
  end

  defp callbacks(test_pid, overrides \\ []) do
    fragments =
      Keyword.get(overrides, :fragments, [fragment("fragment://private/root", :private)])

    base = [
      list_fragment_family: fn context ->
        send(test_pid, {:list_family, context.root_fragment_id})
        {:ok, fragments}
      end,
      insert_invalidation: fn invalidation, context ->
        send(test_pid, {:insert_invalidation, invalidation.fragment_id, context.reason})
        {:ok, invalidation}
      end,
      publish_cluster_invalidation: fn message, context ->
        send(test_pid, {:cluster_publish, message, context.reason})
        :ok
      end,
      revoke_access_edges: fn context ->
        send(
          test_pid,
          {:access_edges_revoked, context.reason, context.tenant_ref, context.source_node_ref}
        )

        {:ok, %{revoked_edge_ids: ["edge://ua/1"], epoch: 43}}
      end,
      invalidate_caches: fn invalidations, context ->
        send(test_pid, {:cache_invalidated, invalidations, context.reason})
        :ok
      end,
      emit_proof: fn proof_token, context ->
        send(test_pid, {:proof, proof_token, context.reason})
        {:ok, proof_token}
      end
    ]

    Keyword.merge(base, Keyword.drop(overrides, [:fragments]))
  end

  defp request(overrides \\ []) do
    %{
      tenant_ref: @tenant_ref,
      installation_ref: @installation_ref,
      trace_id: "trace-invalidate-alpha",
      root_fragment_id: "fragment://private/root",
      reason: :user_deletion,
      effective_at: ~U[2026-04-24 17:00:00Z],
      effective_at_epoch: 42,
      source_node_ref: @node_ref,
      commit_lsn: "16/B374D84C",
      commit_hlc: @commit_hlc,
      invalidate_policy_ref: "policy://invalidate/v1",
      authority_ref: %{"ref" => "governance://invalidate/alpha"},
      evidence_refs: [%{"ref" => "evidence://invalidate/alpha"}],
      policy_ref: nil
    }
    |> Map.merge(Map.new(overrides))
  end

  defp fragment(fragment_id, tier, overrides \\ []) do
    %{
      fragment_id: fragment_id,
      tenant_ref: @tenant_ref,
      tier: tier,
      parent_fragment_id: nil,
      access_projection_hash: "sha256:" <> String.duplicate("a", 64),
      applied_policies: ["policy://phase7/#{tier}"]
    }
    |> Map.merge(Map.new(overrides))
  end
end
