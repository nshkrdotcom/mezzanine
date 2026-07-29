defmodule Mezzanine.WorkflowRuntime.RecoveryControlTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Runs.AcceptCommand
  alias Mezzanine.Work.WorkClass
  alias Mezzanine.WorkflowRuntime.RecoveryControl
  alias Mezzanine.WorkflowRuntime.Store.Postgres

  @hash "sha256:" <> String.duplicate("a", 64)

  setup do
    owner = Sandbox.start_owner!(Repo, shared: true)
    on_exit(fn -> Sandbox.stop_owner(owner) end)
    truncate!()
    {:ok, lineage: lineage_fixture()}
  end

  test "atomically persists optimistic pause/resume and exactly one signal intent", %{
    lineage: lineage
  } do
    command = command("pause-resume", lineage)
    assert {:ok, _acceptance} = Postgres.accept_run(command, repo: Repo)
    assert :ok = RecoveryControl.preflight(repo: Repo)

    assert {:ok, running} =
             control(command, :activate, 1, "activate", %{
               attempt_ref: "attempt://jido/codex/pause-resume/1",
               generation_ref: "generation://mezzanine/pause-resume/1",
               external_operation_ref: "operation://jido/codex/pause-resume/1"
             })

    assert running.control_state == "running"
    assert running.control_row_version == 2
    assert is_nil(running.outbox_ref)

    assert {:ok, pause_requested} = control(command, :pause, 2, "pause")
    assert pause_requested.control_state == "pause_requested"
    assert pause_requested.control_row_version == 3
    assert is_binary(pause_requested.outbox_ref)

    assert {:ok, replay} = control(command, :pause, 2, "pause")
    assert replay.control_row_version == 3
    assert replay.outbox_ref == pause_requested.outbox_ref
    assert replay.idempotent_replay?

    assert {:error, {:stale_control_version, 3}} =
             control(command, :pause, 2, "pause-drift")

    assert {:ok, paused} = control(command, :pause_ack, 3, "pause-ack")
    assert paused.control_state == "paused"
    assert is_nil(paused.outbox_ref)

    assert {:ok, resume_requested} = control(command, :resume, 4, "resume")
    assert resume_requested.control_state == "resume_requested"
    assert is_binary(resume_requested.outbox_ref)

    assert {:ok, resumed} = control(command, :resume_ack, 5, "resume-ack")
    assert resumed.control_state == "running"

    assert {:ok, events} = RecoveryControl.list_events(command.run_ref, repo: Repo)
    assert Enum.map(events, & &1.event_type) == ~w(activate pause pause_ack resume resume_ack)
    assert Enum.map(events, & &1.sequence) == [1, 2, 3, 4, 5]

    assert %{rows: [[2]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM agent_control_signal_outbox WHERE run_ref = $1",
               [command.run_ref]
             )
  end

  test "killed owner is fenced into reconciliation and unknown outcome needs operator recovery",
       %{
         lineage: lineage
       } do
    command = command("killed-owner", lineage)
    assert {:ok, _acceptance} = Postgres.accept_run(command, repo: Repo)
    parent = self()

    owner_pid =
      spawn(fn ->
        result =
          control(command, :activate, 1, "activate", %{
            attempt_ref: "attempt://jido/codex/killed-owner/1",
            generation_ref: "generation://mezzanine/killed-owner/1",
            external_operation_ref: "operation://jido/codex/killed-owner/1"
          })

        send(parent, {:owner_committed, result})
        Process.sleep(:infinity)
      end)

    assert_receive {:owner_committed, {:ok, %{control_state: "running"}}}
    monitor = Process.monitor(owner_pid)
    Process.exit(owner_pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner_pid, :killed}

    assert {:ok, [reconciling]} =
             RecoveryControl.reconcile_on_start("reconciler://node/one",
               repo: Repo,
               stale_seconds: 0,
               lease_seconds: 60
             )

    assert reconciling.control_state == "reconciling"
    assert reconciling.fence_epoch == 2
    assert reconciling.current_attempt_ref == "attempt://jido/codex/killed-owner/1"
    assert reconciling.external_operation_ref == "operation://jido/codex/killed-owner/1"

    attrs = command_attrs("reconcile-not-found")

    assert {:error, {:stale_fence_epoch, 2}} =
             RecoveryControl.reconcile_attempt(
               context(command),
               command.run_ref,
               reconciling.control_row_version,
               1,
               {:ok, :active},
               attrs,
               repo: Repo
             )

    assert {:ok, operator_required} =
             RecoveryControl.reconcile_attempt(
               context(command),
               command.run_ref,
               reconciling.control_row_version,
               2,
               {:error, :not_found},
               attrs,
               repo: Repo
             )

    assert operator_required.control_state == "operator_required"
    assert operator_required.current_attempt_ref == "attempt://jido/codex/killed-owner/1"

    assert {:ok, retry_requested} =
             control(
               command,
               :retry,
               operator_required.control_row_version,
               "operator-retry",
               %{
                 attempt_ref: "attempt://jido/codex/killed-owner/2",
                 external_operation_ref: "operation://jido/codex/killed-owner/2"
               }
             )

    assert retry_requested.control_state == "retry_requested"
    assert retry_requested.control_attempt_sequence == 2
    assert retry_requested.current_attempt_ref == "attempt://jido/codex/killed-owner/2"
  end

  test "retry, supersession, deadline, and cancel retain generation and terminal receipts", %{
    lineage: lineage
  } do
    command = command("supersession", lineage)
    assert {:ok, _acceptance} = Postgres.accept_run(command, repo: Repo)

    assert {:ok, running} =
             control(command, :activate, 1, "activate", %{
               attempt_ref: "attempt://jido/codex/supersession/1",
               generation_ref: "generation://mezzanine/supersession/1",
               external_operation_ref: "operation://jido/codex/supersession/1",
               deadline_at: DateTime.add(DateTime.utc_now(), -60, :second)
             })

    assert {:ok, [deadline]} = RecoveryControl.expire_deadlines(repo: Repo)
    assert deadline.control_state == "cancel_requested"

    assert {:ok, cancelled} =
             control(command, :cancel_ack, deadline.control_row_version, "deadline-ack", %{
               receipt_ref: "receipt://mezzanine/supersession/cancelled"
             })

    assert cancelled.control_state == "cancelled"

    assert {:ok, supersede_requested} =
             control(command, :supersede, cancelled.control_row_version, "supersede", %{
               generation_ref: "generation://mezzanine/supersession/2",
               attempt_ref: "attempt://jido/codex/supersession/2",
               external_operation_ref: "operation://jido/codex/supersession/2",
               deadline_at: DateTime.add(DateTime.utc_now(), 600, :second)
             })

    assert supersede_requested.control_state == "supersede_requested"
    assert supersede_requested.control_generation == 2
    assert supersede_requested.control_attempt_sequence == 1
    assert is_nil(supersede_requested.terminal_receipt_ref)

    assert {:ok, running_generation_two} =
             control(
               command,
               :supersede_ack,
               supersede_requested.control_row_version,
               "supersede-ack"
             )

    assert running_generation_two.control_state == "running"
    assert running_generation_two.generation_ref == "generation://mezzanine/supersession/2"
    assert running.control_generation == 1
  end

  test "signal outbox reclaims killed dispatchers and persists ambiguous delivery retry state", %{
    lineage: lineage
  } do
    command = command("signal-reclaim", lineage)
    assert {:ok, _acceptance} = Postgres.accept_run(command, repo: Repo)

    assert {:ok, _running} =
             control(command, :activate, 1, "activate", %{
               attempt_ref: "attempt://jido/codex/signal-reclaim/1",
               generation_ref: "generation://mezzanine/signal-reclaim/1",
               external_operation_ref: "operation://jido/codex/signal-reclaim/1"
             })

    assert {:ok, _pause} = control(command, :pause, 2, "pause")

    assert {:ok, [claimed]} =
             RecoveryControl.claim_signal_outboxes("dispatcher://one", 1,
               repo: Repo,
               lock_seconds: 0
             )

    assert claimed.state == "dispatching"

    assert {:ok, [reclaimed]} =
             RecoveryControl.claim_signal_outboxes("dispatcher://two", 1,
               repo: Repo,
               lock_seconds: 30
             )

    assert reclaimed.outbox_ref == claimed.outbox_ref
    assert reclaimed.dispatch_fence == claimed.dispatch_fence + 1

    assert {:error, :stale_dispatch_fence} =
             RecoveryControl.complete_signal_outbox(
               claimed.outbox_ref,
               claimed.dispatch_fence,
               {:ok, %{status: "late"}},
               repo: Repo
             )

    assert {:ok, retryable} =
             RecoveryControl.complete_signal_outbox(
               reclaimed.outbox_ref,
               reclaimed.dispatch_fence,
               {:error, :temporal_unavailable},
               repo: Repo,
               retry_seconds: 0
             )

    assert retryable.state == "retryable"
    assert retryable.signal_id == claimed.signal_id
    assert retryable.idempotency_key == claimed.idempotency_key
  end

  defp control(command, action, version, suffix, attrs \\ %{}) do
    RecoveryControl.control(
      context(command),
      command.run_ref,
      action,
      version,
      Map.merge(command_attrs(suffix), attrs),
      repo: Repo
    )
  end

  defp context(command) do
    %{
      tenant_ref: command.tenant_ref,
      actor_ref: "actor://synapse/operator",
      operator_ref: "operator://synapse/one",
      authority_ref: "authority://citadel/control",
      permission_decision_ref: "decision://citadel/control",
      trace_ref: command.trace_ref,
      correlation_ref: command.correlation_ref
    }
  end

  defp command_attrs(suffix) do
    %{
      command_ref: "command://mezzanine/control/#{suffix}",
      idempotency_key: "control:#{suffix}"
    }
  end

  defp lineage_fixture do
    suffix = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    tenant_id = "tenant://mezzanine/recovery/#{suffix}"
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "recovery-#{suffix}",
          name: "Recovery",
          product_family: "synapse",
          configuration: %{},
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, bundle} =
      PolicyBundle.load_bundle(
        %{
          program_id: program.id,
          name: "recovery",
          version: "1.0.0",
          policy_kind: :workflow_md,
          source_ref: "policy://synapse/recovery",
          body: workflow_body(),
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_class} =
      WorkClass.create_work_class(
        %{
          program_id: program.id,
          name: "agent_run",
          kind: "agent_run",
          intake_schema: %{"required" => ["subject_ref", "input_artifact_ref"]},
          policy_bundle_id: bundle.id,
          default_review_profile: %{"required" => false},
          default_run_profile: %{"runtime" => "session"}
        },
        actor: actor,
        tenant: tenant_id
      )

    %{tenant_id: tenant_id, program: program, work_class: work_class}
  end

  defp command(suffix, lineage) do
    AcceptCommand.new!(%{
      command_ref: "command://mezzanine/#{suffix}",
      idempotency_key: "synapse:#{suffix}",
      request_hash: @hash,
      tenant_ref: lineage.tenant_id,
      installation_ref: "installation://acme/synapse/prod",
      actor_ref: "actor://synapse/operator",
      program_id: lineage.program.id,
      work_class_id: lineage.work_class.id,
      subject_ref: "subject://synapse/#{suffix}",
      run_ref: "run://mezzanine/#{suffix}",
      trace_ref: "trace://synapse/#{suffix}",
      correlation_ref: "correlation://synapse/#{suffix}",
      authority_context_ref: "authority-context://synapse/#{suffix}",
      runtime_profile_ref: "runtime-profile://nshkr/local-model",
      tool_catalog_ref: "tool-catalog://synapse/default",
      budget_ref: "budget://synapse/default",
      deadline_at: DateTime.add(DateTime.utc_now(), 3_600, :second),
      expected_revision: 0,
      first_turn: %{
        turn_ref: "turn://synapse/#{suffix}/1",
        subject_ref: "subject://synapse/#{suffix}",
        input_artifact_ref: "artifact://outer-brain/#{suffix}",
        payload_digest: @hash,
        idempotency_key: "synapse:#{suffix}:turn:1",
        sequence: 1,
        row_version: 1
      }
    })
  end

  defp workflow_body do
    """
    ---
    run:
      profile: synapse_agent_run
      runtime_class: session
      capability: agent.turn
      target: nshkr-runtime
    review:
      required: false
      required_decisions: 0
    ---
    Recover one durable Synapse run.
    """
  end

  defp truncate! do
    SQL.query!(
      Repo,
      """
      TRUNCATE programs, policy_bundles, work_classes, work_objects, work_plans,
               control_sessions, run_series, runs, agent_run_commands, agent_turns,
               agent_run_events, agent_run_projections, agent_run_cursors,
               agent_workflow_outbox, agent_run_control_commands,
               agent_run_control_events, agent_control_signal_outbox CASCADE
      """,
      []
    )
  end
end
