defmodule Mezzanine.WorkflowRuntime.DeterministicCodexReceiptTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkflowRuntime.DeterministicCodexReceipt

  @fixture_path "test/fixtures/codex_receipts/phase9_deterministic_receipts.json"

  test "declares deterministic Codex receipt activity contract and no-live-IO posture" do
    contract = DeterministicCodexReceipt.contract()

    assert contract.activity_version == "Mezzanine.DeterministicCodexReceiptActivity.v1"
    assert contract.workflow_runtime_owner == :mezzanine
    assert contract.lower_runtime_shape == :codex
    assert contract.allowed_input_sources == [:local_fixture, :temporal_state_file]
    assert contract.live_external_io_allowed? == false

    assert :provider_adapter in contract.forbidden_live_fields
    assert :linear_adapter in contract.forbidden_live_fields
    assert :github_adapter in contract.forbidden_live_fields
    assert :network_client in contract.forbidden_live_fields
  end

  test "maps deterministic fixture receipts to workflow facts without live IO" do
    state_db = temporal_state_fixture!()

    assert {:ok, result} =
             DeterministicCodexReceipt.run_activity(activity_attrs(state_db))

    assert result.contract_name == "Mezzanine.DeterministicCodexReceiptActivity.v1"
    assert result.owner_repo == :mezzanine
    assert result.io_policy.live_external_io_allowed? == false
    assert result.io_policy.allowed_input_sources == [:local_fixture, :temporal_state_file]
    assert result.temporal_substrate.address == "127.0.0.1:7233"
    assert result.temporal_substrate.namespace == "default"
    assert result.temporal_substrate.state_db_present? == true
    assert result.temporal_substrate.state_db_ref =~ "sha256:"

    facts = Map.new(result.receipt_facts, &{&1.receipt_state, &1})

    assert facts["completed"].fact_kind == :codex_completion
    assert facts["completed"].terminal? == true
    assert facts["completed"].workflow_effect_state == "completed"
    assert facts["completed"].projection_state == "projected"

    assert facts["failed"].fact_kind == :codex_failure
    assert facts["failed"].failure_class == "codex_execution_failed"
    assert facts["failed"].safe_action == "operator_review"

    assert facts["stalled"].fact_kind == :codex_stall
    assert facts["stalled"].heartbeat_state == "stalled"
    assert facts["stalled"].safe_action == "retry_or_cancel"
    assert facts["stalled"].stall_timeout_ms == 60_000

    assert facts["user_input_required"].fact_kind == :codex_user_input_required
    assert facts["user_input_required"].workflow_effect_state == "blocked"
    assert facts["user_input_required"].safe_action == "operator_review"

    assert facts["rate_limited"].fact_kind == :codex_rate_limited
    assert facts["rate_limited"].safe_action == "backoff"
    assert facts["rate_limited"].retry_after_ms == 120_000

    assert result.fact_counts == %{
             completed: 1,
             failed: 1,
             rate_limited: 1,
             stalled: 1,
             user_input_required: 1
           }

    refute inspect(result) =~ File.cwd!()
    refute inspect(result) =~ "raw_provider_payload"
  end

  test "Temporal activity wrapper uses the deterministic fixture mapper" do
    state_db = temporal_state_fixture!()

    assert {:ok, result} =
             Mezzanine.Activities.DeterministicCodexReceipt.perform(activity_attrs(state_db))

    assert result.activity == :deterministic_codex_receipt
    assert result.workflow_effect_state == "deterministic_receipts_mapped"
    assert result.fact_counts.completed == 1
  end

  test "dedupes token retransmission before projection and omits token text" do
    state_db = temporal_state_fixture!()

    assert {:ok, result} =
             DeterministicCodexReceipt.run_activity(activity_attrs(state_db))

    assert result.token_dedupe.accepted_count == 2
    assert result.token_dedupe.duplicate_count == 1
    assert result.token_dedupe.duplicate_event_refs == ["token-event://phase9/1-replayed"]

    assert result.token_dedupe.accepted_event_refs == [
             "token-event://phase9/1",
             "token-event://phase9/2"
           ]

    assert Enum.all?(result.token_dedupe.token_hash_refs, &String.starts_with?(&1, "sha256:"))

    refute inspect(result) =~ "never-project-this-token"
    refute inspect(result) =~ "second-token"
  end

  test "rejects live adapters and non-local fixtures before reading receipts" do
    state_db = temporal_state_fixture!()

    assert {:error, {:live_external_io_forbidden, :provider_adapter}} =
             state_db
             |> activity_attrs()
             |> Map.put(:provider_adapter, FakeProvider)
             |> DeterministicCodexReceipt.run_activity()

    assert {:error, {:non_local_fixture_path, "https://example.invalid/fixture.json"}} =
             state_db
             |> activity_attrs()
             |> Map.put(:fixture_path, "https://example.invalid/fixture.json")
             |> DeterministicCodexReceipt.run_activity()
  end

  defp activity_attrs(state_db) do
    %{
      tenant_ref: "tenant://phase9",
      installation_ref: "installation://phase9",
      workspace_ref: "workspace://phase9",
      workflow_id: "workflow-phase9",
      workflow_run_id: "run-phase9",
      activity_call_ref: "activity://workflow-phase9/codex-receipt",
      fixture_path: @fixture_path,
      temporal_address: "127.0.0.1:7233",
      temporal_namespace: "default",
      temporal_state_db_path: state_db,
      trace_id: "trace-phase9",
      idempotency_key: "codex-receipt:phase9:deterministic",
      release_manifest_ref: "phase9-deterministic-codex-receipt"
    }
  end

  defp temporal_state_fixture! do
    path =
      Path.join(
        System.tmp_dir!(),
        "mezzanine-phase9-temporal-#{System.unique_integer([:positive])}.db"
      )

    File.write!(path, "temporal-state-fixture")
    path
  end
end
