defmodule Mezzanine.WorkflowRuntime.Store.Postgres do
  @moduledoc "Postgres owner store for canonical run, turn, event, projection, cursor, and outbox truth."

  @behaviour Mezzanine.WorkflowRuntime.Store

  alias Ecto.Adapters.SQL
  alias Mezzanine.Runs.{AcceptCommand, Acceptance, Event, EventCursor, WorkflowHandoff}
  alias Mezzanine.WorkControl

  @migration_version 20_260_720_111_500
  @default_namespace "nshkr-production"
  @default_task_queue "nshkr.mezzanine.agent-run.v1"
  @default_workflow_type "mezzanine.agent-run.v1"
  @handoff_columns ~w(outbox_ref event_ref run_ref workflow_ref workflow_type temporal_namespace task_queue idempotency_key state attempt last_error_ref)a

  @impl true
  def capabilities do
    Mezzanine.Persistence.postgres_capability(:mezzanine_run_truth, [
      :commands,
      :runs,
      :turns,
      :events,
      :projections,
      :cursors,
      :workflow_outbox
    ])
  end

  @impl true
  def preflight(opts) do
    repo = repo(opts)

    with {:ok, %{rows: [[1]]}} <- SQL.query(repo, "SELECT 1", []),
         {:ok, %{rows: [[present?]]}} <-
           SQL.query(
             repo,
             "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)",
             [@migration_version]
           ),
         true <- present? do
      :ok
    else
      false -> {:error, {:required_migration_missing, @migration_version}}
      {:error, reason} -> {:error, {:postgres_unavailable, reason}}
      other -> {:error, {:postgres_preflight_failed, other}}
    end
  end

  @impl true
  def health(opts) do
    with :ok <- preflight(opts) do
      {:ok,
       %{
         adapter: :postgres,
         capability: capabilities(),
         migration_version: @migration_version,
         repo: repo(opts),
         restart_safe?: true
       }}
    end
  end

  @impl true
  def accept_run(command, opts) do
    with {:ok, command} <- AcceptCommand.new(command) do
      case transact(repo(opts), fn -> persist_acceptance(command, opts) end) do
        {:ok, acceptance} -> {:ok, acceptance}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @impl true
  def fetch_acceptance(command_ref, opts) when is_binary(command_ref) do
    sql = "SELECT acceptance FROM agent_run_commands WHERE command_ref = $1"

    case SQL.query(repo(opts), sql, [command_ref]) do
      {:ok, %{rows: [[attrs]]}} -> Acceptance.new(attrs)
      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def fetch_projection(run_ref, opts) when is_binary(run_ref) do
    sql = """
    SELECT run_ref, tenant_id, subject_ref, latest_turn_ref, latest_event_ref,
           status, event_sequence, run_revision, projection, updated_at
    FROM agent_run_projections
    WHERE run_ref = $1
    """

    case SQL.query(repo(opts), sql, [run_ref]) do
      {:ok, %{rows: [row]}} -> {:ok, projection(row)}
      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def list_events(run_ref, cursor, opts) when is_binary(run_ref) do
    with {:ok, sequence} <- cursor_sequence(run_ref, cursor),
         {:ok, %{rows: rows}} <-
           SQL.query(
             repo(opts),
             """
             SELECT event_ref, run_ref, tenant_id, event_type, event_version, sequence,
                    command_ref, causation_ref, correlation_ref, payload_ref,
                    payload_digest, recorded_at, row_version
             FROM agent_run_events
             WHERE run_ref = $1 AND sequence > $2
             ORDER BY sequence ASC
             LIMIT $3
             """,
             [run_ref, sequence, Keyword.get(opts, :limit, 100)]
           ) do
      rows
      |> Enum.map(&event_from_row/1)
      |> collect_results()
    end
  end

  @impl true
  def read_cursor(run_ref, opts) when is_binary(run_ref) do
    case SQL.query(
           repo(opts),
           "SELECT run_ref, last_event_ref, sequence FROM agent_run_cursors WHERE run_ref = $1",
           [run_ref]
         ) do
      {:ok, %{rows: [[stored_run_ref, event_ref, sequence]]}} ->
        EventCursor.new(run_ref: stored_run_ref, last_event_ref: event_ref, sequence: sequence)

      {:ok, %{rows: []}} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def claim_workflow_handoffs(lock_owner, limit, opts)
      when is_binary(lock_owner) and is_integer(limit) and limit > 0 do
    expired_dispatch_sql = """
    UPDATE agent_workflow_outbox
    SET state = 'ambiguous',
        last_error_ref = 'error://mezzanine/temporal/dispatcher-lost',
        lock_owner = NULL,
        lock_expires_at = NULL,
        row_version = row_version + 1,
        updated_at = now()
    WHERE state = 'dispatched' AND lock_expires_at < now()
    """

    sql = """
    WITH claimable AS (
      SELECT outbox_ref
      FROM agent_workflow_outbox
      WHERE state = 'pending'
        AND available_at <= now()
        AND (lock_expires_at IS NULL OR lock_expires_at < now())
      ORDER BY available_at, outbox_ref
      FOR UPDATE SKIP LOCKED
      LIMIT $2
    )
    UPDATE agent_workflow_outbox AS outbox
    SET state = 'dispatched',
        attempt = outbox.attempt + 1,
        lock_owner = $1,
        lock_expires_at = now() + interval '30 seconds',
        row_version = outbox.row_version + 1,
        updated_at = now()
    FROM claimable
    WHERE outbox.outbox_ref = claimable.outbox_ref
    RETURNING outbox.outbox_ref, outbox.event_ref, outbox.run_ref, outbox.workflow_ref,
              outbox.workflow_type, outbox.temporal_namespace, outbox.task_queue,
              outbox.idempotency_key, outbox.state, outbox.attempt, outbox.last_error_ref
    """

    transaction = fn ->
      with {:ok, _result} <- SQL.query(repo(opts), expired_dispatch_sql, []),
           {:ok, %{rows: rows}} <- SQL.query(repo(opts), sql, [lock_owner, limit]) do
        rows
      else
        {:error, reason} -> repo(opts).rollback(reason)
      end
    end

    case transact(repo(opts), transaction) do
      {:ok, rows} -> rows |> Enum.map(&handoff_from_row/1) |> collect_results()
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def complete_workflow_handoff(outbox_ref, next_state, error_ref, opts)
      when is_binary(outbox_ref) and next_state in ["acknowledged", "ambiguous"] do
    sql = """
    UPDATE agent_workflow_outbox
    SET state = $2,
        last_error_ref = $3,
        lock_owner = NULL,
        lock_expires_at = NULL,
        row_version = row_version + 1,
        updated_at = now()
    WHERE outbox_ref = $1 AND state = 'dispatched'
    RETURNING outbox_ref, event_ref, run_ref, workflow_ref, workflow_type,
              temporal_namespace, task_queue, idempotency_key, state, attempt, last_error_ref
    """

    case SQL.query(repo(opts), sql, [outbox_ref, next_state, error_ref]) do
      {:ok, %{rows: [row]}} -> handoff_from_row(row)
      {:ok, %{rows: []}} -> {:error, :handoff_state_conflict}
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_acceptance(command, opts) do
    case insert_command(command, opts) do
      :inserted ->
        started = create_canonical_run!(command, opts)
        facts = acceptance_facts(command, started, opts)
        insert_turn!(command, started, facts, opts)
        insert_event!(started, facts, opts)
        insert_projection!(command, started, facts, opts)
        insert_cursor!(started, facts, opts)
        insert_handoff!(started, facts, opts)
        complete_command!(command, started, facts, opts)
        facts.acceptance

      :duplicate ->
        existing_acceptance!(command, opts)
    end
  end

  defp insert_command(command, opts) do
    sql = """
    INSERT INTO agent_run_commands
      (command_ref, tenant_id, installation_ref, idempotency_key, request_hash,
       run_ref, authority_context_ref, state, acceptance, row_version, inserted_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, 1, $9, $9)
    ON CONFLICT (tenant_id, installation_ref, idempotency_key) DO NOTHING
    RETURNING command_ref
    """

    params = [
      command.command_ref,
      command.tenant_ref,
      command.installation_ref,
      command.idempotency_key,
      command.request_hash,
      command.run_ref,
      command.authority_context_ref,
      %{},
      now(opts)
    ]

    case SQL.query!(repo(opts), sql, params).rows do
      [[_command_ref]] -> :inserted
      [] -> :duplicate
    end
  end

  defp existing_acceptance!(command, opts) do
    sql = """
    SELECT request_hash, acceptance
    FROM agent_run_commands
    WHERE tenant_id = $1 AND installation_ref = $2 AND idempotency_key = $3
    FOR UPDATE
    """

    case SQL.query!(repo(opts), sql, [
           command.tenant_ref,
           command.installation_ref,
           command.idempotency_key
         ]).rows do
      [[stored_hash, acceptance]] when stored_hash == command.request_hash ->
        case Acceptance.new(acceptance) do
          {:ok, value} -> value
          {:error, reason} -> repo(opts).rollback({:corrupt_acceptance, reason})
        end

      [[_stored_hash, _acceptance]] ->
        repo(opts).rollback(:idempotency_conflict)

      [] ->
        repo(opts).rollback(:idempotency_record_missing)
    end
  end

  defp create_canonical_run!(command, opts) do
    attrs = %{
      program_id: command.program_id,
      work_class_id: command.work_class_id,
      external_ref: command.subject_ref,
      title: "Synapse agent run",
      description: "Accepted through the AppKit agent-intake boundary",
      source_kind: "app_kit_agent_intake",
      payload: acceptance_payload(command),
      normalized_payload: acceptance_payload(command),
      trace_id: command.trace_ref,
      actor_ref: command.actor_ref,
      installation_ref: command.installation_ref,
      idempotency_key: command.idempotency_key,
      runtime_profile_ref: command.runtime_profile_ref,
      run_ref: command.run_ref,
      owner_transaction?: true
    }

    with {:ok, prepared} <- WorkControl.prepare_run_request(command.tenant_ref, attrs),
         {:ok, started} <-
           WorkControl.start_run_for_subject(command.tenant_ref, prepared.work_object.id, attrs),
         true <- started.run.external_ref == command.run_ref do
      started
    else
      false -> repo(opts).rollback(:run_identity_conflict)
      {:error, reason} -> repo(opts).rollback(reason)
    end
  end

  defp acceptance_payload(command) do
    %{
      "subject_ref" => command.subject_ref,
      "initial_input_artifact_ref" => command.first_turn.input_artifact_ref,
      "actor_ref" => command.actor_ref,
      "correlation_ref" => command.correlation_ref,
      "runtime_profile_ref" => command.runtime_profile_ref,
      "tool_catalog_ref" => command.tool_catalog_ref,
      "budget_ref" => command.budget_ref,
      "authority_context_ref" => command.authority_context_ref,
      "deadline_at" => deadline_value(command.deadline_at),
      "expected_revision" => command.expected_revision
    }
  end

  defp insert_turn!(command, started, facts, opts) do
    turn = command.first_turn

    SQL.query!(
      repo(opts),
      """
      INSERT INTO agent_turns
        (turn_ref, run_id, tenant_id, subject_ref, input_artifact_ref, payload_digest,
         idempotency_key, sequence, status, provider_attempt_ref, row_version,
         inserted_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'accepted',NULL,$9,$10,$10)
      """,
      [
        turn.turn_ref,
        dump_uuid(started.run.id),
        command.tenant_ref,
        turn.subject_ref,
        turn.input_artifact_ref,
        turn.payload_digest,
        turn.idempotency_key,
        turn.sequence,
        turn.row_version,
        facts.now
      ]
    )
  end

  defp insert_event!(started, facts, opts) do
    event = facts.event

    SQL.query!(
      repo(opts),
      """
      INSERT INTO agent_run_events
        (event_ref, run_id, run_ref, tenant_id, event_type, event_version, sequence,
         command_ref, causation_ref, correlation_ref, payload_ref, payload_digest,
         recorded_at, row_version)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
      """,
      [
        event.event_ref,
        dump_uuid(started.run.id),
        event.run_ref,
        event.tenant_ref,
        event.event_type,
        event.event_version,
        event.sequence,
        event.command_ref,
        event.causation_ref,
        event.correlation_ref,
        event.payload_ref,
        event.payload_digest,
        event.recorded_at,
        event.row_version
      ]
    )
  end

  defp insert_projection!(command, started, facts, opts) do
    projection = %{
      "acceptance" => Acceptance.dump(facts.acceptance),
      "input_artifact_ref" => command.first_turn.input_artifact_ref,
      "runtime_profile_ref" => command.runtime_profile_ref,
      "trace_ref" => command.trace_ref
    }

    SQL.query!(
      repo(opts),
      """
      INSERT INTO agent_run_projections
        (run_id, run_ref, tenant_id, work_object_id, subject_ref, latest_turn_ref,
         latest_event_ref, status, event_sequence, run_revision, projection,
         inserted_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,'accepted',1,1,$8,$9,$9)
      """,
      [
        dump_uuid(started.run.id),
        command.run_ref,
        command.tenant_ref,
        dump_uuid(started.work_object.id),
        command.subject_ref,
        command.first_turn.turn_ref,
        facts.event.event_ref,
        projection,
        facts.now
      ]
    )
  end

  defp insert_cursor!(started, facts, opts) do
    cursor = facts.acceptance.cursor

    SQL.query!(
      repo(opts),
      """
      INSERT INTO agent_run_cursors
        (run_id, run_ref, last_event_ref, sequence, row_version, inserted_at, updated_at)
      VALUES ($1,$2,$3,$4,1,$5,$5)
      """,
      [
        dump_uuid(started.run.id),
        cursor.run_ref,
        cursor.last_event_ref,
        cursor.sequence,
        facts.now
      ]
    )
  end

  defp insert_handoff!(started, facts, opts) do
    handoff = facts.handoff

    SQL.query!(
      repo(opts),
      """
      INSERT INTO agent_workflow_outbox
      (outbox_ref, event_ref, run_id, run_ref, workflow_ref, workflow_type,
       temporal_namespace, task_queue, idempotency_key, state, attempt,
       last_error_ref, available_at, row_version, inserted_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now(),1,$13,$13)
      """,
      [
        handoff.outbox_ref,
        handoff.event_ref,
        dump_uuid(started.run.id),
        handoff.run_ref,
        handoff.workflow_ref,
        handoff.workflow_type,
        handoff.temporal_namespace,
        handoff.task_queue,
        handoff.idempotency_key,
        handoff.state,
        handoff.attempt,
        handoff.last_error_ref,
        facts.now
      ]
    )
  end

  defp complete_command!(command, started, facts, opts) do
    case SQL.query!(
           repo(opts),
           """
           UPDATE agent_run_commands
           SET run_id = $2, state = 'accepted', acceptance = $3, updated_at = $4
           WHERE command_ref = $1 AND state = 'pending'
           RETURNING command_ref
           """,
           [
             command.command_ref,
             dump_uuid(started.run.id),
             Acceptance.dump(facts.acceptance),
             facts.now
           ]
         ).rows do
      [[_command_ref]] -> :ok
      [] -> repo(opts).rollback(:command_state_conflict)
    end
  end

  defp acceptance_facts(command, _started, opts) do
    token = digest_token({command.tenant_ref, command.installation_ref, command.idempotency_key})
    event_ref = "event://mezzanine/#{token}/1"
    outbox_ref = "outbox://mezzanine/#{token}/workflow-start"
    workflow_ref = "workflow://temporal/#{token}"
    now = now(opts)

    event =
      Event.new!(
        event_ref: event_ref,
        run_ref: command.run_ref,
        tenant_ref: command.tenant_ref,
        event_type: "run_accepted",
        event_version: 1,
        sequence: 1,
        command_ref: command.command_ref,
        correlation_ref: command.correlation_ref,
        payload_ref: command.first_turn.input_artifact_ref,
        payload_digest: command.request_hash,
        recorded_at: now,
        row_version: 1
      )

    handoff =
      WorkflowHandoff.new!(
        outbox_ref: outbox_ref,
        event_ref: event_ref,
        run_ref: command.run_ref,
        workflow_ref: workflow_ref,
        workflow_type: Keyword.get(opts, :workflow_type, @default_workflow_type),
        temporal_namespace: Keyword.get(opts, :temporal_namespace, @default_namespace),
        task_queue: Keyword.get(opts, :task_queue, @default_task_queue),
        idempotency_key: "#{command.idempotency_key}:workflow-start",
        state: "pending",
        attempt: 0
      )

    acceptance =
      Acceptance.new!(
        command_ref: command.command_ref,
        run_ref: command.run_ref,
        turn_ref: command.first_turn.turn_ref,
        event_ref: event_ref,
        workflow_outbox_ref: outbox_ref,
        cursor: %{run_ref: command.run_ref, last_event_ref: event_ref, sequence: 1},
        run_revision: 1,
        state: "accepted"
      )

    %{acceptance: acceptance, event: event, handoff: handoff, now: now}
  end

  defp event_from_row(row) do
    row
    |> Enum.zip(event_columns())
    |> Map.new(fn {value, key} -> {key, value} end)
    |> Map.update!(:recorded_at, &as_datetime/1)
    |> Event.new()
  end

  defp event_columns,
    do:
      ~w(event_ref run_ref tenant_ref event_type event_version sequence command_ref causation_ref correlation_ref payload_ref payload_digest recorded_at row_version)a

  defp handoff_from_row(row) do
    row
    |> Enum.zip(@handoff_columns)
    |> Map.new(fn {value, key} -> {key, value} end)
    |> WorkflowHandoff.new()
  end

  defp projection([
         run_ref,
         tenant_ref,
         subject_ref,
         turn_ref,
         event_ref,
         status,
         sequence,
         revision,
         body,
         updated_at
       ]) do
    %{
      run_ref: run_ref,
      tenant_ref: tenant_ref,
      subject_ref: subject_ref,
      latest_turn_ref: turn_ref,
      latest_event_ref: event_ref,
      status: status,
      event_sequence: sequence,
      run_revision: revision,
      projection: body,
      updated_at: updated_at
    }
  end

  defp cursor_sequence(_run_ref, nil), do: {:ok, 0}

  defp cursor_sequence(run_ref, %EventCursor{run_ref: run_ref, sequence: sequence}),
    do: {:ok, sequence}

  defp cursor_sequence(_run_ref, %EventCursor{}), do: {:error, :cursor_run_mismatch}
  defp cursor_sequence(_run_ref, _cursor), do: {:error, :invalid_event_cursor}

  defp as_datetime(%DateTime{} = value), do: value
  defp as_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")

  defp collect_results(results) do
    Enum.reduce_while(results, {:ok, []}, fn
      {:ok, value}, {:ok, acc} -> {:cont, {:ok, [value | acc]}}
      {:error, reason}, _acc -> {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp digest_token(term) do
    term
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp now(opts), do: Keyword.get_lazy(opts, :now, &DateTime.utc_now/0)

  defp deadline_value(nil), do: nil
  defp deadline_value(%DateTime{} = deadline), do: DateTime.to_iso8601(deadline)

  defp dump_uuid(value), do: Ecto.UUID.dump!(value)

  defp transact(repo, fun) do
    repo.transaction(fun)
  rescue
    _error in [DBConnection.ConnectionError, Ecto.ConstraintError, Postgrex.Error] ->
      {:error, :postgres_write_failed}
  catch
    :exit, _reason -> {:error, :postgres_unavailable}
  end

  defp repo(opts), do: Keyword.get(opts, :repo, Mezzanine.OpsDomain.Repo)
end
