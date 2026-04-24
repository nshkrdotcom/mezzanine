defmodule Mezzanine.Audit.RetrospectiveAuditTest do
  use Mezzanine.Audit.DataCase, async: false

  alias Mezzanine.Audit.{MemoryProofToken, MemoryProofTokenStore, RetrospectiveAudit}

  @event_time ~U[2026-04-23 08:00:00.000000Z]
  @historical_epoch 42
  @current_epoch 84

  setup do
    Process.put(:retro_fragments, %{})
    Process.put(:retro_current_candidates, [])
    Process.put(:retro_denied, MapSet.new())
    Process.put(:retro_policy_error, nil)
    Process.put(:retro_current_policy_refs, [])
    :ok
  end

  test "verify_as_of_recall replays clean recall predicates and emits signed audit artifact" do
    put_fragment(@historical_epoch, fragment("fragment-a"))

    assert {:ok, _proof} =
             valid_attrs(%{
               proof_id: "proof-retro-clean",
               fragment_ids: ["fragment-a"],
               access_projection_hashes: ["sha256:" <> String.duplicate("a", 64)],
               metadata: %{"snapshot_epoch" => @historical_epoch}
             })
             |> MemoryProofTokenStore.emit()

    assert {:ok, report} =
             RetrospectiveAudit.verify_as_of_recall(
               "proof-retro-clean",
               providers()
             )

    assert report.mode == :verify_as_of_recall
    assert report.status == :verified
    assert report.snapshot_epoch == @historical_epoch
    assert report.admitted_fragment_ids == ["fragment-a"]
    assert report.source_proof_token.proof_id == "proof-retro-clean"
    assert report.audit_artifact.signature =~ ~r/^sha256:[a-f0-9]{64}$/

    assert_received {:retrospective_audit_artifact,
                     %{mode: :verify_as_of_recall, proof_id: "proof-retro-clean"}}
  end

  test "re_evaluate_under_current recomputes current accessibility and flags denied fragments" do
    put_fragment(@historical_epoch, fragment("fragment-a"))
    put_fragment(@current_epoch, fragment("fragment-a"))
    Process.put(:retro_denied, %{@current_epoch => MapSet.new(["fragment-a"])})

    assert {:ok, _proof} =
             valid_attrs(%{
               proof_id: "proof-retro-current-denied",
               fragment_ids: ["fragment-a"],
               access_projection_hashes: ["sha256:" <> String.duplicate("a", 64)]
             })
             |> MemoryProofTokenStore.emit()

    assert {:ok, report} =
             RetrospectiveAudit.re_evaluate_under_current(
               "proof-retro-current-denied",
               providers()
             )

    assert report.mode == :re_evaluate_under_current
    assert report.current_epoch == @current_epoch
    assert report.admitted_fragment_ids == []

    assert [%{fragment_id: "fragment-a", reason: :accessibility_predicate_failed}] =
             report.inadmissible_fragments
  end

  test "drift_report diffs historical and current admission, policy, and transform changes" do
    put_fragment(@historical_epoch, fragment("fragment-a"))
    put_fragment(@current_epoch, fragment("fragment-a", transform_pipeline: [%{kind: "redact"}]))
    put_fragment(@current_epoch, fragment("fragment-b"))

    Process.put(:retro_current_candidates, ["fragment-a", "fragment-b"])
    Process.put(:retro_denied, %{@current_epoch => MapSet.new(["fragment-a"])})
    Process.put(:retro_current_policy_refs, [%{"id" => "policy-read", "version" => 2}])

    assert {:ok, _proof} =
             valid_attrs(%{
               proof_id: "proof-retro-drift",
               fragment_ids: ["fragment-a"],
               access_projection_hashes: ["sha256:" <> String.duplicate("a", 64)]
             })
             |> MemoryProofTokenStore.emit()

    assert {:ok, report} =
             RetrospectiveAudit.drift_report(
               "proof-retro-drift",
               providers()
             )

    assert report.mode == :drift_report
    assert report.newly_inadmissible_fragment_ids == ["fragment-a"]
    assert report.newly_admissible_fragment_ids == ["fragment-b"]
    assert [%{fragment_id: "fragment-a"}] = report.transform_changes

    assert [
             %{
               historical: [%{"id" => "policy-read", "version" => 1}],
               current: [%{"id" => "policy-read", "version" => 2}]
             }
           ] = report.policy_version_changes
  end

  test "rejects tampered proof tokens before predicate replay" do
    assert {:ok, proof} =
             valid_attrs(%{proof_id: "proof-retro-tampered"})
             |> MemoryProofToken.new()

    tampered = %{proof | proof_hash: String.duplicate("0", 64)}
    Process.put(:retro_proof_store_result, {:ok, tampered})

    assert {:error, {:proof_hash_mismatch, _details}} =
             RetrospectiveAudit.verify_as_of_recall(
               "proof-retro-tampered",
               providers(proof_store: __MODULE__.TamperedProofStore)
             )
  end

  test "rejects missing fragments, expired policies, and missing M7A ordering evidence" do
    assert {:ok, _proof} =
             valid_attrs(%{proof_id: "proof-retro-missing-fragment"})
             |> MemoryProofTokenStore.emit()

    assert {:error, {:missing_fragments, ["fragment-a"]}} =
             RetrospectiveAudit.verify_as_of_recall(
               "proof-retro-missing-fragment",
               providers()
             )

    put_fragment(@historical_epoch, fragment("fragment-a"))
    Process.put(:retro_policy_error, {:policy_ref_not_effective, %{"id" => "policy-read"}})

    assert {:error, {:policy_ref_not_effective, %{"id" => "policy-read"}}} =
             RetrospectiveAudit.verify_as_of_recall(
               "proof-retro-missing-fragment",
               providers()
             )

    missing_order_attrs = valid_attrs(%{proof_id: "proof-retro-missing-ordering"})
    Process.put(:retro_proof_store_result, {:ok, Map.delete(missing_order_attrs, :commit_hlc)})

    assert {:error, {:missing_proof_token_fields, fields}} =
             RetrospectiveAudit.verify_as_of_recall(
               "proof-retro-missing-ordering",
               providers(proof_store: __MODULE__.TamperedProofStore)
             )

    assert :commit_hlc in fields
  end

  defp providers(overrides \\ []) do
    Keyword.merge(
      [
        fragment_store: __MODULE__.FragmentStore,
        access_graph_store: __MODULE__.AccessGraphStore,
        policy_registry: __MODULE__.PolicyRegistry,
        artifact_store: __MODULE__.ArtifactStore
      ],
      overrides
    )
  end

  defp put_fragment(epoch, attrs) do
    fragments = Process.get(:retro_fragments, %{})
    fragment_id = Map.fetch!(attrs, :fragment_id)
    Process.put(:retro_fragments, Map.put(fragments, {epoch, fragment_id}, attrs))
  end

  defp fragment(fragment_id, overrides \\ []) do
    Map.merge(
      %{
        fragment_id: fragment_id,
        tenant_ref: "tenant-alpha",
        source_node_ref: "node://mez_a@127.0.0.1/node-a",
        t_epoch: 40,
        access_agents: ["agent-alpha"],
        access_resources: ["resource-alpha"],
        access_scopes: ["scope-alpha"],
        access_projection_hash: "sha256:" <> String.duplicate("a", 64),
        applied_policies: [%{"id" => "policy-read", "version" => 1}],
        transform_pipeline: [],
        parent_fragment_id: nil
      },
      Map.new(overrides)
    )
  end

  defp valid_attrs(overrides) do
    %{
      proof_id: "proof-retro-default",
      proof_hash_version: "m7a.v1",
      kind: :recall,
      tenant_ref: "tenant-alpha",
      installation_id: "installation-alpha",
      subject_id: "subject-alpha",
      execution_id: "execution-alpha",
      user_ref: "user-alpha",
      agent_ref: "agent-alpha",
      t_event: @event_time,
      epoch_used: @historical_epoch,
      policy_refs: [%{"id" => "policy-read", "version" => 1}],
      fragment_ids: ["fragment-a"],
      transform_hashes: ["sha256:" <> String.duplicate("1", 64)],
      access_projection_hashes: ["sha256:" <> String.duplicate("a", 64)],
      source_node_ref: "node://mez_a@127.0.0.1/node-a",
      commit_lsn: "16/B374D848",
      commit_hlc: %{
        "w" => 1_776_947_200_000_000_000,
        "l" => 0,
        "n" => "node://mez_a@127.0.0.1/node-a"
      },
      trace_id: "trace-retro",
      metadata: %{"operation_ref" => "retrospective-audit-test"}
    }
    |> Map.merge(overrides)
  end

  defmodule TamperedProofStore do
    def fetch(_proof_id), do: Process.get(:retro_proof_store_result)
  end

  defmodule FragmentStore do
    def fetch_fragments(_tenant_ref, fragment_ids, opts) do
      epoch = Keyword.fetch!(opts, :snapshot_epoch)
      fragments = Process.get(:retro_fragments, %{})

      fragment_ids
      |> Enum.map(&Map.get(fragments, {epoch, &1}))
      |> Enum.reject(&is_nil/1)
    end

    def current_epoch(_tenant_ref), do: 84

    def current_candidate_fragment_ids(_token, _opts) do
      Process.get(:retro_current_candidates, [])
    end

    def parent_chain(_tenant_ref, _fragment_id, _opts), do: {:ok, []}
  end

  defmodule AccessGraphStore do
    def replay_views(_tenant_ref, _user_ref, _agent_ref, epoch, tuple) do
      denied = denied_fragment_ids(epoch)
      fragment_id = Map.fetch!(tuple, :fragment_id)

      %{
        snapshot_epoch: epoch,
        graph_admissible?: not MapSet.member?(denied, fragment_id),
        access_agents: MapSet.new(tuple.access_agents),
        access_resources: MapSet.new(tuple.access_resources),
        access_scopes: MapSet.new(tuple.access_scopes)
      }
    end

    defp denied_fragment_ids(epoch) do
      case Process.get(:retro_denied, MapSet.new()) do
        %MapSet{} = denied ->
          denied

        denied_by_epoch when is_map(denied_by_epoch) ->
          Map.get(denied_by_epoch, epoch, MapSet.new())
      end
    end
  end

  defmodule PolicyRegistry do
    def validate_refs_at(policy_refs, _context, _opts) do
      case Process.get(:retro_policy_error) do
        nil -> {:ok, policy_refs}
        error -> {:error, error}
      end
    end

    def current_policy_refs(_context, _opts) do
      {:ok, Process.get(:retro_current_policy_refs, [])}
    end
  end

  defmodule ArtifactStore do
    def emit(report, _opts) do
      send(self(), {:retrospective_audit_artifact, report})
      {:ok, %{artifact_id: "audit-artifact://" <> report.proof_id}}
    end
  end
end
