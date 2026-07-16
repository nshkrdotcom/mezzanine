defmodule Mezzanine.WorkflowRuntime.StorePostgresTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.Runs.AcceptCommand
  alias Mezzanine.WorkflowRuntime.Store.Postgres

  @hash "sha256:" <> String.duplicate("a", 64)

  setup tags do
    if tags[:repo_restart] do
      :ok = Sandbox.mode(Mezzanine.Repo, :auto)
      truncate!()

      on_exit(fn ->
        if Process.whereis(Mezzanine.Repo) == nil do
          {:ok, pid} = Mezzanine.Repo.start_link()
          Process.unlink(pid)
        end

        Sandbox.mode(Mezzanine.Repo, :manual)
      end)
    else
      :ok = Sandbox.checkout(Mezzanine.Repo)
      truncate!()
    end

    :ok
  end

  test "atomically persists run, first turn, command, event, projection, cursor, and outbox" do
    assert :ok = Postgres.preflight(repo: Mezzanine.Repo)
    command = command("one")

    assert {:ok, acceptance} = Postgres.accept_run(command, repo: Mezzanine.Repo)
    assert acceptance.state == "accepted"
    assert acceptance.cursor.sequence == 1

    for table <- ~w(
      mezzanine_run_commands
      mezzanine_runs
      mezzanine_turns
      mezzanine_run_events
      mezzanine_run_projections
      mezzanine_run_cursors
      mezzanine_workflow_outbox
    ) do
      assert %{rows: [[1]]} = SQL.query!(Mezzanine.Repo, "SELECT count(*) FROM #{table}", [])
    end

    assert {:ok, ^acceptance} =
             Postgres.fetch_acceptance(command.command_ref, repo: Mezzanine.Repo)

    assert {:ok, projection} =
             Postgres.fetch_projection(command.run_ref, repo: Mezzanine.Repo)

    assert projection.latest_turn_ref == command.first_turn.turn_ref
    assert projection.latest_event_ref == acceptance.event_ref
    assert projection.event_sequence == 1

    assert {:ok, cursor} = Postgres.read_cursor(command.run_ref, repo: Mezzanine.Repo)
    assert cursor == acceptance.cursor

    assert {:ok, [event]} = Postgres.list_events(command.run_ref, nil, repo: Mezzanine.Repo)
    assert event.event_ref == acceptance.event_ref
    assert event.sequence == 1
    assert {:ok, []} = Postgres.list_events(command.run_ref, cursor, repo: Mezzanine.Repo)
  end

  test "returns the committed acceptance for an exact duplicate and rejects hash conflicts" do
    command = command("duplicate")

    assert {:ok, first} = Postgres.accept_run(command, repo: Mezzanine.Repo)
    assert {:ok, ^first} = Postgres.accept_run(command, repo: Mezzanine.Repo)

    %AcceptCommand{} = conflict_command = command("conflict")

    conflicting = %{
      conflict_command
      | idempotency_key: command.idempotency_key,
        request_hash: "sha256:" <> String.duplicate("b", 64),
        tenant_ref: command.tenant_ref,
        installation_ref: command.installation_ref
    }

    assert {:error, :idempotency_conflict} =
             Postgres.accept_run(conflicting, repo: Mezzanine.Repo)

    assert %{rows: [[1]]} =
             SQL.query!(Mezzanine.Repo, "SELECT count(*) FROM mezzanine_run_commands", [])
  end

  test "claims only committed outbox rows and durably records the Temporal outcome" do
    command = command("handoff")
    assert {:ok, acceptance} = Postgres.accept_run(command, repo: Mezzanine.Repo)

    assert {:ok, [handoff]} =
             Postgres.claim_workflow_handoffs("test-node", 10, repo: Mezzanine.Repo)

    assert handoff.outbox_ref == acceptance.workflow_outbox_ref
    assert handoff.state == "dispatched"
    assert handoff.attempt == 1
    assert handoff.temporal_namespace == "nshkr-production"
    assert handoff.task_queue == "nshkr.mezzanine.agent-run.v1"

    assert {:ok, acknowledged} =
             Postgres.complete_workflow_handoff(
               handoff.outbox_ref,
               "acknowledged",
               nil,
               repo: Mezzanine.Repo
             )

    assert acknowledged.state == "acknowledged"

    assert {:ok, []} =
             Postgres.claim_workflow_handoffs("other-node", 10, repo: Mezzanine.Repo)
  end

  test "marks an expired dispatched handoff ambiguous instead of silently replaying it" do
    command = command("expired-handoff")
    assert {:ok, acceptance} = Postgres.accept_run(command, repo: Mezzanine.Repo)

    assert {:ok, [_handoff]} =
             Postgres.claim_workflow_handoffs("lost-node", 10, repo: Mezzanine.Repo)

    SQL.query!(
      Mezzanine.Repo,
      """
      UPDATE mezzanine_workflow_outbox
      SET lock_expires_at = now() - interval '1 second'
      WHERE outbox_ref = $1
      """,
      [acceptance.workflow_outbox_ref]
    )

    assert {:ok, []} =
             Postgres.claim_workflow_handoffs("replacement-node", 10, repo: Mezzanine.Repo)

    assert %{rows: [["ambiguous", error_ref, nil, nil]]} =
             SQL.query!(
               Mezzanine.Repo,
               """
               SELECT state, last_error_ref, lock_owner, lock_expires_at
               FROM mezzanine_workflow_outbox
               WHERE outbox_ref = $1
               """,
               [acceptance.workflow_outbox_ref]
             )

    assert error_ref == "error://mezzanine/temporal/dispatcher-lost"
  end

  @tag :repo_restart
  test "committed acceptance survives a repository restart" do
    command = command("restart")
    assert {:ok, acceptance} = Postgres.accept_run(command, repo: Mezzanine.Repo)

    :ok = Supervisor.stop(Mezzanine.Repo)
    {:ok, pid} = Mezzanine.Repo.start_link()
    Process.unlink(pid)

    assert {:ok, ^acceptance} =
             Postgres.fetch_acceptance(command.command_ref, repo: Mezzanine.Repo)
  end

  defp command(suffix) do
    AcceptCommand.new!(%{
      command_ref: "command://mezzanine/#{suffix}",
      idempotency_key: "synapse:#{suffix}",
      request_hash: @hash,
      tenant_ref: "tenant://acme",
      installation_ref: "installation://acme/synapse/prod",
      actor_ref: "actor://synapse/operator",
      subject_ref: "subject://synapse/#{suffix}",
      run_ref: "run://mezzanine/#{suffix}",
      trace_ref: "trace://synapse/#{suffix}",
      correlation_ref: "correlation://synapse/#{suffix}",
      authority_context_ref: "authority-context://synapse/#{suffix}",
      runtime_profile_ref: "runtime-profile://nshkr/local-model",
      tool_catalog_ref: "tool-catalog://synapse/default",
      budget_ref: "budget://synapse/default",
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

  defp truncate! do
    SQL.query!(
      Mezzanine.Repo,
      """
      TRUNCATE mezzanine_workflow_outbox, mezzanine_run_cursors,
               mezzanine_run_projections, mezzanine_run_events, mezzanine_turns,
               mezzanine_run_commands, mezzanine_runs CASCADE
      """,
      []
    )
  end
end
