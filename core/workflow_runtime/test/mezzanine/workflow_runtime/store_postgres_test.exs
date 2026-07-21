defmodule Mezzanine.WorkflowRuntime.StorePostgresTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Runs.AcceptCommand
  alias Mezzanine.Work.WorkClass
  alias Mezzanine.WorkflowRuntime.{ModelTurnCompletion, ModelTurnStart, ProviderEvent}
  alias Mezzanine.WorkflowRuntime.Store.Postgres

  @hash "sha256:" <> String.duplicate("a", 64)

  setup tags do
    if tags[:repo_restart] do
      :ok = Sandbox.mode(Repo, :auto)
      truncate!()

      on_exit(fn ->
        if Process.whereis(Repo) == nil do
          {:ok, pid} = Repo.start_link()
          Process.unlink(pid)
        end

        truncate!()
        Sandbox.mode(Repo, :manual)
      end)
    else
      owner = Sandbox.start_owner!(Repo, shared: true)
      on_exit(fn -> Sandbox.stop_owner(owner) end)
      truncate!()
    end

    {:ok, lineage: lineage_fixture()}
  end

  test "atomically persists canonical work/run lineage and acceptance records", %{
    lineage: lineage
  } do
    assert :ok = Postgres.preflight(repo: Repo)
    command = command("one", lineage)

    assert {:ok, acceptance} = Postgres.accept_run(command, repo: Repo)
    assert acceptance.state == "accepted"
    assert acceptance.cursor.sequence == 1
    tenant_id = lineage.tenant_id
    run_ref = command.run_ref

    for table <- ~w(
          work_objects
          work_plans
          control_sessions
          run_series
          runs
          agent_run_commands
          agent_turns
          agent_run_events
          agent_run_projections
        ) do
      assert %{rows: [[1]]} =
               SQL.query!(Repo, "SELECT count(*) FROM #{table} WHERE tenant_id = $1", [tenant_id])
    end

    assert %{rows: [[1]]} =
             SQL.query!(Repo, "SELECT count(*) FROM agent_run_cursors WHERE run_ref = $1", [
               run_ref
             ])

    assert %{rows: [[1]]} =
             SQL.query!(Repo, "SELECT count(*) FROM agent_workflow_outbox WHERE run_ref = $1", [
               run_ref
             ])

    assert %{rows: [[run_id, ^tenant_id, ^run_ref]]} =
             SQL.query!(
               Repo,
               "SELECT id, tenant_id, external_ref FROM runs WHERE external_ref = $1",
               [
                 run_ref
               ]
             )

    assert %{rows: [[^run_id]]} =
             SQL.query!(Repo, "SELECT run_id FROM agent_run_commands WHERE command_ref = $1", [
               command.command_ref
             ])

    assert %{rows: [[payload]]} =
             SQL.query!(
               Repo,
               "SELECT normalized_payload FROM work_objects WHERE tenant_id = $1",
               [
                 tenant_id
               ]
             )

    assert payload["actor_ref"] == command.actor_ref
    assert payload["deadline_at"] == DateTime.to_iso8601(command.deadline_at)
    assert payload["authority_context_ref"] == command.authority_context_ref

    assert {:ok, ^acceptance} = Postgres.fetch_acceptance(command.command_ref, repo: Repo)
    assert {:ok, projection} = Postgres.fetch_projection(command.run_ref, repo: Repo)
    assert projection.latest_turn_ref == command.first_turn.turn_ref
    assert projection.latest_event_ref == acceptance.event_ref
    assert projection.event_sequence == 1
    assert %DateTime{} = projection.updated_at

    assert {:ok, cursor} = Postgres.read_cursor(command.run_ref, repo: Repo)
    assert cursor == acceptance.cursor

    assert {:ok, [event]} = Postgres.list_events(command.run_ref, nil, repo: Repo)
    assert event.event_ref == acceptance.event_ref
    assert event.sequence == 1
    assert {:ok, []} = Postgres.list_events(command.run_ref, cursor, repo: Repo)
  end

  test "returns the committed acceptance for stable identity and rejects hash conflicts", %{
    lineage: lineage
  } do
    command = command("duplicate", lineage)

    assert {:ok, first} = Postgres.accept_run(command, repo: Repo)
    assert {:ok, ^first} = Postgres.accept_run(command, repo: Repo)

    reissued_command = command("reissued", lineage)

    reissued = %{
      reissued_command
      | idempotency_key: command.idempotency_key,
        request_hash: command.request_hash,
        tenant_ref: command.tenant_ref,
        installation_ref: command.installation_ref
    }

    assert {:ok, ^first} = Postgres.accept_run(reissued, repo: Repo)

    conflict_command = command("conflict", lineage)

    conflicting = %{
      conflict_command
      | idempotency_key: command.idempotency_key,
        request_hash: "sha256:" <> String.duplicate("b", 64),
        tenant_ref: command.tenant_ref,
        installation_ref: command.installation_ref
    }

    assert {:error, :idempotency_conflict} = Postgres.accept_run(conflicting, repo: Repo)

    assert %{rows: [[1]]} =
             SQL.query!(Repo, "SELECT count(*) FROM agent_run_commands WHERE tenant_id = $1", [
               lineage.tenant_id
             ])

    assert %{rows: [[1]]} =
             SQL.query!(Repo, "SELECT count(*) FROM runs WHERE tenant_id = $1", [
               lineage.tenant_id
             ])
  end

  test "rolls back every acceptance row when canonical lineage is invalid", %{lineage: lineage} do
    command = %{command("invalid-lineage", lineage) | work_class_id: Ecto.UUID.generate()}

    assert {:error, _reason} = Postgres.accept_run(command, repo: Repo)

    for table <- ~w(
          work_objects
          work_plans
          control_sessions
          run_series
          runs
          agent_run_commands
          agent_turns
          agent_run_events
          agent_run_projections
        ) do
      assert %{rows: [[0]]} =
               SQL.query!(Repo, "SELECT count(*) FROM #{table} WHERE tenant_id = $1", [
                 lineage.tenant_id
               ])
    end

    assert %{rows: [[0]]} =
             SQL.query!(Repo, "SELECT count(*) FROM agent_run_cursors WHERE run_ref = $1", [
               command.run_ref
             ])

    assert %{rows: [[0]]} =
             SQL.query!(Repo, "SELECT count(*) FROM agent_workflow_outbox WHERE run_ref = $1", [
               command.run_ref
             ])
  end

  test "preflight requires this store migration without rejecting later owner migrations" do
    SQL.query!(
      Repo,
      "INSERT INTO schema_migrations (version, inserted_at) VALUES ($1, now())",
      [20_260_720_999_999]
    )

    assert :ok = Postgres.preflight(repo: Repo)
  end

  test "claims only committed outbox rows and durably records the Temporal outcome", %{
    lineage: lineage
  } do
    command = command("handoff", lineage)
    assert {:ok, acceptance} = Postgres.accept_run(command, repo: Repo)

    assert {:ok, [handoff]} = Postgres.claim_workflow_handoffs("test-node", 10, repo: Repo)
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
               repo: Repo
             )

    assert acknowledged.state == "acknowledged"
    assert {:ok, []} = Postgres.claim_workflow_handoffs("other-node", 10, repo: Repo)
  end

  test "marks an expired dispatched handoff ambiguous instead of replaying it", %{
    lineage: lineage
  } do
    command = command("expired-handoff", lineage)
    assert {:ok, acceptance} = Postgres.accept_run(command, repo: Repo)
    assert {:ok, [_handoff]} = Postgres.claim_workflow_handoffs("lost-node", 10, repo: Repo)

    SQL.query!(
      Repo,
      """
      UPDATE agent_workflow_outbox
      SET lock_expires_at = now() - interval '1 second'
      WHERE outbox_ref = $1
      """,
      [acceptance.workflow_outbox_ref]
    )

    assert {:ok, []} = Postgres.claim_workflow_handoffs("replacement-node", 10, repo: Repo)

    assert %{rows: [["ambiguous", error_ref, nil, nil]]} =
             SQL.query!(
               Repo,
               """
               SELECT state, last_error_ref, lock_owner, lock_expires_at
               FROM agent_workflow_outbox
               WHERE outbox_ref = $1
               """,
               [acceptance.workflow_outbox_ref]
             )

    assert error_ref == "error://mezzanine/temporal/dispatcher-lost"
  end

  @tag :repo_restart
  test "committed canonical acceptance survives a repository restart", %{lineage: lineage} do
    command = command("restart", lineage)
    assert {:ok, acceptance} = Postgres.accept_run(command, repo: Repo)

    :ok = Supervisor.stop(Repo)
    {:ok, pid} = Repo.start_link()
    Process.unlink(pid)

    assert {:ok, ^acceptance} = Postgres.fetch_acceptance(command.command_ref, repo: Repo)
    assert {:ok, projection} = Postgres.fetch_projection(command.run_ref, repo: Repo)
    assert projection.status == "accepted"
    assert {:ok, cursor} = Postgres.read_cursor(command.run_ref, repo: Repo)
    assert cursor == acceptance.cursor
  end

  test "commits provisional provider boundaries before terminal model-turn projection", %{
    lineage: lineage
  } do
    command = command("model-turn", lineage)
    assert {:ok, _acceptance} = Postgres.accept_run(command, repo: Repo)

    start = model_turn_start(command, "model-turn")
    assert {:ok, running} = Postgres.start_model_turn(start, repo: Repo)
    assert running.state == "running"
    assert running.provider_attempt_ref == start.provider_attempt_ref
    assert running.context_artifact_ref == start.context_artifact_ref
    assert running.decision_ref == start.decision_ref
    assert running.grant_ref == start.grant_ref

    delta = provider_event(command, "model-turn", 1, "inference.response_delta")
    assert {:ok, provisional} = Postgres.append_provider_event(delta, repo: Repo)
    assert provisional.commit_state == "provisional"

    assert {:error, :not_found} =
             Postgres.read_model_turn_cursor(command.first_turn.turn_ref, repo: Repo)

    assert {:ok, [listed_delta]} =
             Postgres.list_provider_events(command.first_turn.turn_ref, 0, repo: Repo)

    assert listed_delta.commit_state == "provisional"

    assert {:ok, committed_delta} =
             Postgres.commit_provider_event(delta.event_ref, repo: Repo)

    assert committed_delta.commit_state == "committed"

    assert {:ok, cursor} =
             Postgres.read_model_turn_cursor(command.first_turn.turn_ref, repo: Repo)

    assert cursor.sequence == 1
    assert cursor.last_provider_event_ref == delta.event_ref

    terminal = provider_event(command, "model-turn", 2, "inference.attempt_completed")
    assert {:ok, provisional_terminal} = Postgres.append_provider_event(terminal, repo: Repo)
    assert provisional_terminal.commit_state == "provisional"

    completion = model_turn_completion(command, "model-turn")

    assert {:error, :provisional_provider_events_present} =
             Postgres.complete_model_turn(completion, repo: Repo)

    assert {:ok, committed_terminal} =
             Postgres.commit_provider_event(terminal.event_ref, repo: Repo)

    assert committed_terminal.commit_state == "committed"
    assert {:ok, completed} = Postgres.complete_model_turn(completion, repo: Repo)
    assert completed.state == "completed"
    assert completed.reply_publication_ref == completion.reply_publication_ref
    assert completed.reply_artifact_ref == completion.reply_artifact_ref
    assert completed.continuation_context_ref == completion.continuation_context_ref
    assert completed.cursor.sequence == 2

    assert {:ok, ^completed} = Postgres.fetch_model_turn(command.first_turn.turn_ref, repo: Repo)

    assert {:ok, [^committed_terminal]} =
             Postgres.list_provider_events(command.first_turn.turn_ref, 1, repo: Repo)

    assert {:ok, projection} = Postgres.fetch_projection(command.run_ref, repo: Repo)
    assert projection.status == "completed"
    assert projection.projection["model_turn"]["state"] == "completed"

    assert %{rows: [["completed", "completed"]]} =
             SQL.query!(
               Repo,
               """
               SELECT turns.status, runs.status
               FROM agent_turns AS turns
               JOIN runs ON runs.id = turns.run_id
               WHERE turns.turn_ref = $1
               """,
               [command.first_turn.turn_ref]
             )
  end

  test "fails closed on non-canonical, out-of-order, or non-terminal model lineage", %{
    lineage: lineage
  } do
    command = command("model-negative", lineage)
    assert {:ok, _acceptance} = Postgres.accept_run(command, repo: Repo)

    wrong_tenant = %{
      model_turn_start(command, "model-negative")
      | tenant_ref: "tenant://mezzanine/other"
    }

    assert {:error, :canonical_turn_not_found} =
             Postgres.start_model_turn(wrong_tenant, repo: Repo)

    start = model_turn_start(command, "model-negative")
    assert {:ok, _running} = Postgres.start_model_turn(start, repo: Repo)
    assert {:ok, _same_running} = Postgres.start_model_turn(start, repo: Repo)

    out_of_order = provider_event(command, "model-negative", 2, "inference.attempt_completed")

    assert {:error, :provider_event_binding_conflict} =
             Postgres.append_provider_event(out_of_order, repo: Repo)

    delta = provider_event(command, "model-negative", 1, "inference.response_delta")
    assert {:ok, stored_delta} = Postgres.append_provider_event(delta, repo: Repo)
    assert {:ok, ^stored_delta} = Postgres.append_provider_event(delta, repo: Repo)

    conflicting_delta = %{delta | payload_ref: "artifact://jido/provider-event/conflict"}

    assert {:error, :provider_event_identity_conflict} =
             Postgres.append_provider_event(conflicting_delta, repo: Repo)

    terminal = provider_event(command, "model-negative", 2, "inference.attempt_completed")
    assert {:ok, _provisional_terminal} = Postgres.append_provider_event(terminal, repo: Repo)

    assert {:error, :non_contiguous_provider_event_commit} =
             Postgres.commit_provider_event(terminal.event_ref, repo: Repo)

    assert {:ok, _committed_delta} = Postgres.commit_provider_event(delta.event_ref, repo: Repo)

    assert {:error, :provisional_provider_events_present} =
             Postgres.complete_model_turn(
               model_turn_completion(command, "model-negative"),
               repo: Repo
             )

    assert {:ok, _committed_terminal} =
             Postgres.commit_provider_event(terminal.event_ref, repo: Repo)

    wrong_attempt = %{
      model_turn_completion(command, "model-negative")
      | provider_attempt_ref: "attempt://jido/gemini/wrong"
    }

    assert {:error, :provider_attempt_binding_conflict} =
             Postgres.complete_model_turn(wrong_attempt, repo: Repo)
  end

  test "does not convert a committed delta into terminal success", %{lineage: lineage} do
    command = command("non-terminal", lineage)
    assert {:ok, _acceptance} = Postgres.accept_run(command, repo: Repo)

    assert {:ok, _running} =
             Postgres.start_model_turn(model_turn_start(command, "non-terminal"), repo: Repo)

    delta = provider_event(command, "non-terminal", 1, "inference.response_delta")
    assert {:ok, _provisional} = Postgres.append_provider_event(delta, repo: Repo)
    assert {:ok, _committed} = Postgres.commit_provider_event(delta.event_ref, repo: Repo)

    assert {:error, :terminal_success_event_required} =
             Postgres.complete_model_turn(
               model_turn_completion(command, "non-terminal"),
               repo: Repo
             )

    assert {:ok, running} = Postgres.fetch_model_turn(command.first_turn.turn_ref, repo: Repo)
    assert running.state == "running"
  end

  @tag :repo_restart
  test "completed model-turn lineage and committed cursor survive repository restart", %{
    lineage: lineage
  } do
    command = command("model-restart", lineage)
    assert {:ok, _acceptance} = Postgres.accept_run(command, repo: Repo)

    assert {:ok, _running} =
             Postgres.start_model_turn(model_turn_start(command, "model-restart"), repo: Repo)

    terminal = provider_event(command, "model-restart", 1, "inference.attempt_completed")
    assert {:ok, _provisional} = Postgres.append_provider_event(terminal, repo: Repo)

    assert {:ok, committed_terminal} =
             Postgres.commit_provider_event(terminal.event_ref, repo: Repo)

    completion = model_turn_completion(command, "model-restart")
    assert {:ok, completed} = Postgres.complete_model_turn(completion, repo: Repo)

    :ok = Supervisor.stop(Repo)
    {:ok, pid} = Repo.start_link()
    Process.unlink(pid)

    assert {:ok, ^completed} = Postgres.fetch_model_turn(command.first_turn.turn_ref, repo: Repo)

    assert {:ok, [^committed_terminal]} =
             Postgres.list_provider_events(command.first_turn.turn_ref, 0, repo: Repo)

    assert {:ok, cursor} =
             Postgres.read_model_turn_cursor(command.first_turn.turn_ref, repo: Repo)

    assert cursor.sequence == 1
    assert cursor.last_provider_event_ref == terminal.event_ref
  end

  defp lineage_fixture do
    suffix = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    tenant_id = "tenant://mezzanine/store/#{suffix}"
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "agent-intake-#{suffix}",
          name: "Agent Intake",
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
          name: "agent_intake",
          version: "1.0.0",
          policy_kind: :workflow_md,
          source_ref: "policy://synapse/agent-intake",
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
      deadline_at: ~U[2026-08-01 00:00:00Z],
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

  defp model_turn_start(command, suffix) do
    ModelTurnStart.new!(%{
      tenant_ref: command.tenant_ref,
      run_ref: command.run_ref,
      turn_ref: command.first_turn.turn_ref,
      context_artifact_ref: "artifact://outer-brain/context/#{suffix}",
      context_digest: @hash,
      prompt_artifact_ref: "artifact://outer-brain/prompt/#{suffix}",
      decision_ref: "decision://citadel/model/#{suffix}",
      grant_ref: "grant://citadel/model/#{suffix}",
      provider_attempt_ref: "attempt://jido/gemini/#{suffix}",
      provider_family: "gemini",
      model_ref: "model://google/gemini-2.5-flash",
      operation_ref: "operation://gemini/stream-generate"
    })
  end

  defp provider_event(command, suffix, sequence, event_type) do
    ProviderEvent.new!(%{
      event_ref: "event://jido/gemini/#{suffix}/#{sequence}",
      run_ref: command.run_ref,
      turn_ref: command.first_turn.turn_ref,
      provider_attempt_ref: "attempt://jido/gemini/#{suffix}",
      sequence: sequence,
      event_type: event_type,
      stream: "assistant",
      payload_ref: "artifact://jido/provider-event/#{suffix}/#{sequence}",
      payload_digest: @hash,
      observed_at: DateTime.add(~U[2026-08-01 00:00:00Z], sequence, :second)
    })
  end

  defp model_turn_completion(command, suffix) do
    ModelTurnCompletion.new!(%{
      turn_ref: command.first_turn.turn_ref,
      provider_attempt_ref: "attempt://jido/gemini/#{suffix}",
      reply_publication_ref: "publication://outer-brain/#{suffix}",
      reply_artifact_ref: "artifact://outer-brain/reply/#{suffix}",
      continuation_context_ref: "artifact://outer-brain/context/#{suffix}/next",
      continuation_context_digest: @hash
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
    Accept the first durable Synapse agent turn.
    """
  end

  defp truncate! do
    SQL.query!(
      Repo,
      """
      TRUNCATE programs, policy_bundles, work_classes, work_objects, work_plans,
               control_sessions, run_series, runs, agent_run_commands, agent_turns,
               agent_run_events, agent_run_projections, agent_run_cursors,
               agent_workflow_outbox CASCADE
      """,
      []
    )
  end
end
