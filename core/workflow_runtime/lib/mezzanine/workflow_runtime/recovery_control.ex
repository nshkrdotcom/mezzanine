defmodule Mezzanine.WorkflowRuntime.RecoveryControl do
  @moduledoc """
  Durable owner for run control, recovery, deadlines, and signal delivery intent.

  Every command locks the canonical run projection, verifies the expected
  control row version, changes control state, appends a control event, and
  inserts any required Temporal signal into the durable outbox in one Postgres
  transaction. Provider effects are never replayed by this module.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Runs.Event
  alias Mezzanine.WorkflowRuntime.ControlSignalProtocol

  @migration_version 20_260_728_130_000
  @release_manifest_ref "release://nshkr/mezzanine-recovery-control-v1"
  @terminal_states ~w(completed failed cancelled)
  @actions [
    :activate,
    :pause,
    :pause_ack,
    :resume,
    :resume_ack,
    :cancel,
    :cancel_ack,
    :retry,
    :retry_ack,
    :supersede,
    :supersede_ack,
    :deadline,
    :outcome_unknown,
    :restart_reconcile,
    :reconcile_active,
    :reconcile_completed,
    :reconcile_failed,
    :reconcile_not_found,
    :reconcile_unavailable,
    :operator_fail
  ]
  @row_columns [
    :run_ref,
    :tenant_ref,
    :installation_ref,
    :subject_ref,
    :run_status,
    :control_state,
    :control_generation,
    :control_attempt_sequence,
    :control_sequence,
    :control_row_version,
    :current_attempt_ref,
    :generation_ref,
    :external_operation_ref,
    :deadline_at,
    :fence_epoch,
    :reconciliation_attempts,
    :reconcile_owner,
    :reconcile_lease_expires_at,
    :next_reconcile_at,
    :terminal_receipt_ref,
    :last_control_error,
    :control_updated_at,
    :updated_at,
    :projection
  ]
  @outbox_columns [
    :outbox_ref,
    :run_ref,
    :tenant_ref,
    :command_ref,
    :workflow_ref,
    :signal_id,
    :signal_name,
    :signal_version,
    :signal_payload,
    :payload_digest,
    :authority_ref,
    :idempotency_key,
    :state,
    :attempt,
    :available_at,
    :dispatch_fence,
    :last_error_ref,
    :row_version
  ]
  @public_row_keys [
    :run_ref,
    :tenant_ref,
    :subject_ref,
    :run_status,
    :control_state,
    :control_generation,
    :control_attempt_sequence,
    :control_sequence,
    :control_row_version,
    :current_attempt_ref,
    :generation_ref,
    :external_operation_ref,
    :deadline_at,
    :fence_epoch,
    :reconciliation_attempts,
    :reconcile_owner,
    :reconcile_lease_expires_at,
    :next_reconcile_at,
    :terminal_receipt_ref,
    :last_control_error,
    :control_updated_at
  ]

  @type result :: {:ok, map()} | {:error, term()}

  @spec migration_version() :: pos_integer()
  def migration_version, do: @migration_version

  @spec preflight(keyword()) :: :ok | {:error, term()}
  def preflight(opts \\ []) do
    with {:ok, %{rows: [[1]]}} <- SQL.query(repo(opts), "SELECT 1", []),
         {:ok, %{rows: [[true]]}} <-
           SQL.query(
             repo(opts),
             "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)",
             [@migration_version]
           ) do
      :ok
    else
      {:ok, %{rows: [[false]]}} -> {:error, {:required_migration_missing, @migration_version}}
      {:error, reason} -> {:error, {:postgres_unavailable, reason}}
      other -> {:error, {:postgres_preflight_failed, other}}
    end
  end

  @doc """
  Applies one optimistic owner command.

  Required context refs are `tenant_ref`, `actor_ref`, `authority_ref`,
  `permission_decision_ref`, `trace_ref`, and `correlation_ref`. Mutating
  attributes require `command_ref` and `idempotency_key`.
  """
  @spec control(
          map() | keyword(),
          String.t(),
          atom(),
          pos_integer(),
          map() | keyword(),
          keyword()
        ) ::
          result()
  def control(context, run_ref, action, expected_version, attrs \\ %{}, opts \\ [])

  def control(context, run_ref, action, expected_version, attrs, opts)
      when is_binary(run_ref) and action in @actions and is_integer(expected_version) and
             expected_version > 0 do
    context = normalize(context)
    attrs = normalize(attrs)

    with :ok <- validate_context(context),
         :ok <- validate_command_attrs(attrs) do
      transaction(repo(opts), fn ->
        control_in_transaction!(
          context,
          run_ref,
          action,
          expected_version,
          attrs,
          opts
        )
      end)
    end
  end

  def control(_context, _run_ref, _action, _expected_version, _attrs, _opts),
    do: {:error, :invalid_control_command}

  @spec fetch(String.t(), keyword()) :: result()
  def fetch(run_ref, opts \\ []) when is_binary(run_ref) do
    case SQL.query(repo(opts), projection_select() <> " WHERE projection.run_ref = $1", [run_ref]) do
      {:ok, %{rows: [row]}} -> {:ok, row |> row_from_result() |> public_row()}
      {:ok, %{rows: []}} -> {:error, :run_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec list_events(String.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def list_events(run_ref, opts \\ []) when is_binary(run_ref) do
    case SQL.query(
           repo(opts),
           """
           SELECT event_ref, run_ref, tenant_id, sequence, command_ref, event_type,
                  from_state, to_state, attempt_ref, generation_ref,
                  external_operation_ref, fence_epoch, payload_digest, metadata,
                  recorded_at
           FROM agent_run_control_events
           WHERE run_ref = $1
           ORDER BY sequence
           """,
           [run_ref]
         ) do
      {:ok, %{rows: rows}} -> {:ok, Enum.map(rows, &event_from_result/1)}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Maps provider observation to reconciliation only; it never dispatches an effect."
  @spec reconcile_attempt(
          map() | keyword(),
          String.t(),
          pos_integer(),
          pos_integer(),
          term(),
          map() | keyword(),
          keyword()
        ) :: result()
  def reconcile_attempt(
        context,
        run_ref,
        expected_version,
        expected_fence_epoch,
        provider_result,
        attrs,
        opts \\ []
      ) do
    attrs =
      attrs
      |> normalize()
      |> Map.put(:expected_fence_epoch, expected_fence_epoch)

    {action, attrs} =
      case provider_result do
        {:ok, :active} ->
          {:reconcile_active, attrs}

        {:ok, {:completed, receipt_ref}} ->
          {:reconcile_completed, Map.put(attrs, :receipt_ref, receipt_ref)}

        {:ok, {:failed, receipt_ref}} ->
          {:reconcile_failed, Map.put(attrs, :receipt_ref, receipt_ref)}

        {:error, :not_found} ->
          {:reconcile_not_found, attrs}

        {:error, reason} ->
          {:reconcile_unavailable, Map.put(attrs, :error_ref, error_ref(reason))}

        other ->
          {:reconcile_unavailable,
           Map.put(attrs, :error_ref, error_ref({:invalid_provider_status, other}))}
      end

    control(context, run_ref, action, expected_version, attrs, opts)
  end

  @doc """
  Claims restart reconciliation using database time and monotonic fence epochs.

  Running effects whose owner disappeared become `reconciling`; no new attempt
  or external operation identity is created.
  """
  @spec reconcile_on_start(String.t(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def reconcile_on_start(owner, opts \\ []) when is_binary(owner) and owner != "" do
    limit = Keyword.get(opts, :limit, 20)
    stale_seconds = Keyword.get(opts, :stale_seconds, 60)
    lease_seconds = Keyword.get(opts, :lease_seconds, 30)

    with {:ok, %{rows: candidates}} <-
           SQL.query(
             repo(opts),
             """
             SELECT projection.run_ref, projection.tenant_id,
                    command.installation_ref, projection.control_row_version,
                    projection.fence_epoch
             FROM agent_run_projections AS projection
             JOIN agent_run_commands AS command ON command.run_ref = projection.run_ref
             WHERE projection.external_operation_ref IS NOT NULL
               AND (
                 projection.control_state = 'outcome_unknown'
                 OR (
                   projection.control_state = 'reconciling'
                   AND (
                     projection.reconcile_lease_expires_at IS NULL
                     OR projection.reconcile_lease_expires_at < clock_timestamp()
                   )
                 )
                 OR (
                   projection.control_state = 'running'
                   AND COALESCE(projection.control_updated_at, projection.updated_at)
                     < clock_timestamp() - ($1 * interval '1 second')
                 )
               )
             ORDER BY COALESCE(
               projection.next_reconcile_at,
               projection.control_updated_at,
               projection.updated_at
             )
             LIMIT $2
             """,
             [stale_seconds, limit]
           ) do
      candidates
      |> Enum.map(fn [run_ref, tenant_ref, installation_ref, version, fence_epoch] ->
        next_fence = fence_epoch + 1

        context =
          system_context(
            tenant_ref,
            installation_ref,
            "restart-reconciler",
            run_ref,
            next_fence
          )

        attrs = %{
          command_ref: "command://mezzanine/restart/#{digest_token({run_ref, next_fence})}",
          idempotency_key: "restart-reconcile:#{run_ref}:#{next_fence}",
          reconcile_owner: owner,
          reconcile_lease_seconds: lease_seconds
        }

        control(context, run_ref, :restart_reconcile, version, attrs, opts)
      end)
      |> collect_successes()
    end
  end

  @doc "Transitions due nonterminal rows using database time and queues a durable cancel signal."
  @spec expire_deadlines(keyword()) :: {:ok, [map()]} | {:error, term()}
  def expire_deadlines(opts \\ []) do
    limit = Keyword.get(opts, :limit, 20)

    with {:ok, %{rows: candidates}} <-
           SQL.query(
             repo(opts),
             """
             SELECT projection.run_ref, projection.tenant_id,
                    command.installation_ref, projection.control_row_version,
                    projection.deadline_at
             FROM agent_run_projections AS projection
             JOIN agent_run_commands AS command ON command.run_ref = projection.run_ref
             WHERE projection.deadline_at IS NOT NULL
               AND projection.deadline_at <= clock_timestamp()
               AND projection.control_state NOT IN ('completed', 'failed', 'cancelled')
             ORDER BY projection.deadline_at
             LIMIT $1
             """,
             [limit]
           ) do
      candidates
      |> Enum.map(fn [run_ref, tenant_ref, installation_ref, version, deadline_at] ->
        deadline_token = deadline_at |> as_datetime() |> DateTime.to_iso8601()
        context = system_context(tenant_ref, installation_ref, "deadline", run_ref, version)

        attrs = %{
          command_ref: "command://mezzanine/deadline/#{digest_token({run_ref, deadline_token})}",
          idempotency_key: "deadline:#{run_ref}:#{deadline_token}",
          reason: "deadline_expired"
        }

        control(context, run_ref, :deadline, version, attrs, opts)
      end)
      |> collect_successes()
    end
  end

  @doc "Claims a bounded post-commit signal-outbox batch with an expiring dispatch fence."
  @spec claim_signal_outboxes(String.t(), pos_integer(), keyword()) ::
          {:ok, [map()]} | {:error, term()}
  def claim_signal_outboxes(lock_owner, limit, opts \\ [])
      when is_binary(lock_owner) and lock_owner != "" and is_integer(limit) and limit > 0 do
    lock_seconds = Keyword.get(opts, :lock_seconds, 30)

    transaction(repo(opts), fn ->
      SQL.query!(
        repo(opts),
        """
        UPDATE agent_control_signal_outbox
        SET state = 'retryable',
            available_at = clock_timestamp(),
            lock_owner = NULL,
            lock_expires_at = NULL,
            last_error_ref = 'error://mezzanine/temporal/dispatcher-lost',
            row_version = row_version + 1,
            updated_at = clock_timestamp()
        WHERE state = 'dispatching' AND lock_expires_at < clock_timestamp()
        """,
        []
      )

      rows =
        SQL.query!(
          repo(opts),
          """
          WITH claimable AS (
            SELECT outbox_ref
            FROM agent_control_signal_outbox
            WHERE state IN ('queued', 'retryable')
              AND available_at <= clock_timestamp()
            ORDER BY available_at, outbox_ref
            FOR UPDATE SKIP LOCKED
            LIMIT $2
          )
          UPDATE agent_control_signal_outbox AS outbox
          SET state = 'dispatching',
              attempt = outbox.attempt + 1,
              dispatch_fence = outbox.dispatch_fence + 1,
              lock_owner = $1,
              lock_expires_at = clock_timestamp() + ($3 * interval '1 second'),
              row_version = outbox.row_version + 1,
              updated_at = clock_timestamp()
          FROM claimable
          WHERE outbox.outbox_ref = claimable.outbox_ref
          RETURNING outbox.outbox_ref, outbox.run_ref, outbox.tenant_id,
                    outbox.command_ref, outbox.workflow_ref, outbox.signal_id,
                    outbox.signal_name, outbox.signal_version, outbox.signal_payload,
                    outbox.payload_digest, outbox.authority_ref,
                    outbox.idempotency_key, outbox.state, outbox.attempt,
                    outbox.available_at, outbox.dispatch_fence,
                    outbox.last_error_ref, outbox.row_version
          """,
          [lock_owner, limit, lock_seconds]
        ).rows

      Enum.map(rows, &outbox_from_result/1)
    end)
  end

  @doc "Persists a dispatch result only while the caller still owns the exact dispatch fence."
  @spec complete_signal_outbox(String.t(), pos_integer(), term(), keyword()) :: result()
  def complete_signal_outbox(outbox_ref, dispatch_fence, outcome, opts \\ [])
      when is_binary(outbox_ref) and is_integer(dispatch_fence) and dispatch_fence > 0 do
    {state, retry_seconds, error} =
      case outcome do
        {:ok, _receipt} ->
          {"delivered", nil, nil}

        {:error, {:invalid_request, reason}} ->
          {"terminal_failure", nil, error_ref(reason)}

        {:error, :workflow_not_found} ->
          {"operator_required", nil, error_ref(:workflow_not_found)}

        {:error, reason} ->
          {"retryable", Keyword.get(opts, :retry_seconds, 30), error_ref(reason)}
      end

    available_sql =
      if retry_seconds do
        "clock_timestamp() + ($5 * interval '1 second')"
      else
        "available_at"
      end

    sql = """
    UPDATE agent_control_signal_outbox
    SET state = $3,
        available_at = #{available_sql},
        last_error_ref = $4,
        delivered_at = CASE WHEN $3 = 'delivered' THEN clock_timestamp() ELSE delivered_at END,
        lock_owner = NULL,
        lock_expires_at = NULL,
        row_version = row_version + 1,
        updated_at = clock_timestamp()
    WHERE outbox_ref = $1 AND state = 'dispatching' AND dispatch_fence = $2
    RETURNING outbox_ref, run_ref, tenant_id, command_ref, workflow_ref, signal_id,
              signal_name, signal_version, signal_payload, payload_digest,
              authority_ref, idempotency_key, state, attempt, available_at,
              dispatch_fence, last_error_ref, row_version
    """

    params =
      if retry_seconds,
        do: [outbox_ref, dispatch_fence, state, error, retry_seconds],
        else: [outbox_ref, dispatch_fence, state, error]

    case SQL.query(repo(opts), sql, params) do
      {:ok, %{rows: [row]}} -> {:ok, outbox_from_result(row)}
      {:ok, %{rows: []}} -> {:error, :stale_dispatch_fence}
      {:error, reason} -> {:error, reason}
    end
  end

  defp control_in_transaction!(context, run_ref, action, expected_version, attrs, opts) do
    row = locked_row!(run_ref, opts)
    ensure_tenant!(row, context, opts)
    request_digest = request_digest(context, run_ref, action, expected_version, attrs)
    action_name = Atom.to_string(action)

    case existing_command(row, attrs.idempotency_key, opts) do
      {:ok, %{request_digest: ^request_digest, action: ^action_name, result: result}} ->
        Map.put(result, :idempotent_replay?, true)

      {:ok, _conflict} ->
        repo(opts).rollback(:idempotency_conflict)

      :error ->
        if row.control_row_version != expected_version do
          repo(opts).rollback({:stale_control_version, row.control_row_version})
        end

        now = database_now!(opts)
        transition = transition!(row, action, attrs, now, opts)
        sequence = row.control_sequence + 1
        next_version = row.control_row_version + 1
        event_ref = control_event_ref(run_ref, sequence)

        outbox =
          maybe_build_outbox(
            row,
            context,
            attrs,
            transition,
            action,
            sequence,
            event_ref,
            now,
            opts
          )

        updated =
          transition
          |> Map.put(:control_sequence, sequence)
          |> Map.put(:control_row_version, next_version)
          |> Map.put(:control_updated_at, now)

        persist_projection!(row, updated, opts)
        persist_event!(row, updated, attrs, action, event_ref, request_digest, now, opts)
        persist_timeline_event!(row, context, attrs, event_ref, request_digest, now, opts)
        if outbox, do: persist_outbox!(outbox, now, opts)

        result =
          updated
          |> public_row()
          |> Map.merge(%{
            command_ref: attrs.command_ref,
            event_ref: event_ref,
            outbox_ref: if(outbox, do: outbox.outbox_ref),
            idempotent_replay?: false
          })

        persist_command!(
          row,
          attrs,
          action,
          expected_version,
          request_digest,
          result,
          now,
          opts
        )

        result
    end
  end

  defp transition!(row, action, attrs, now, opts) do
    case transition(row, action, attrs, now) do
      {:ok, updated} -> updated
      {:error, reason} -> repo(opts).rollback(reason)
    end
  end

  defp transition(%{control_state: "accepted"} = row, :activate, attrs, _now) do
    with {:ok, attempt_ref} <- required_ref(attrs, :attempt_ref),
         {:ok, generation_ref} <- required_ref(attrs, :generation_ref),
         {:ok, external_ref} <- required_ref(attrs, :external_operation_ref),
         :ok <- valid_deadline(Map.get(attrs, :deadline_at)) do
      {:ok,
       row
       |> transition_to("running")
       |> Map.merge(%{
         control_attempt_sequence: 1,
         current_attempt_ref: attempt_ref,
         generation_ref: generation_ref,
         external_operation_ref: external_ref,
         deadline_at: Map.get(attrs, :deadline_at, row.deadline_at),
         last_control_error: nil
       })}
    end
  end

  defp transition(%{control_state: "running"} = row, :pause, _attrs, _now),
    do: {:ok, transition_to(row, "pause_requested")}

  defp transition(%{control_state: "pause_requested"} = row, :pause_ack, _attrs, _now),
    do: {:ok, transition_to(row, "paused")}

  defp transition(%{control_state: "paused"} = row, :resume, _attrs, _now),
    do: {:ok, transition_to(row, "resume_requested")}

  defp transition(%{control_state: "resume_requested"} = row, :resume_ack, _attrs, _now),
    do: {:ok, transition_to(row, "running")}

  defp transition(%{control_state: state} = row, :cancel, _attrs, _now)
       when state in ~w(
              accepted running pause_requested paused resume_requested
              retry_requested supersede_requested operator_required
            ),
       do: {:ok, transition_to(row, "cancel_requested")}

  defp transition(%{control_state: "cancel_requested"} = row, :cancel_ack, attrs, _now) do
    with {:ok, receipt_ref} <- required_ref(attrs, :receipt_ref) do
      {:ok,
       row
       |> transition_to("cancelled")
       |> Map.put(:terminal_receipt_ref, receipt_ref)}
    end
  end

  defp transition(%{control_state: state} = row, :retry, attrs, _now)
       when state in ~w(failed operator_required) do
    with {:ok, attempt_ref} <- new_ref(attrs, :attempt_ref, row.current_attempt_ref),
         {:ok, external_ref} <-
           new_ref(attrs, :external_operation_ref, row.external_operation_ref) do
      {:ok,
       row
       |> transition_to("retry_requested")
       |> Map.merge(%{
         control_attempt_sequence: row.control_attempt_sequence + 1,
         current_attempt_ref: attempt_ref,
         external_operation_ref: external_ref,
         terminal_receipt_ref: nil,
         last_control_error: nil,
         reconcile_owner: nil,
         reconcile_lease_expires_at: nil,
         next_reconcile_at: nil
       })}
    end
  end

  defp transition(%{control_state: "retry_requested"} = row, :retry_ack, _attrs, _now),
    do: {:ok, transition_to(row, "running")}

  defp transition(%{control_state: state} = row, :supersede, attrs, _now)
       when state not in ~w(outcome_unknown reconciling supersede_requested) do
    with {:ok, generation_ref} <- new_ref(attrs, :generation_ref, row.generation_ref),
         {:ok, attempt_ref} <- new_ref(attrs, :attempt_ref, row.current_attempt_ref),
         {:ok, external_ref} <-
           new_ref(attrs, :external_operation_ref, row.external_operation_ref),
         :ok <- valid_deadline(Map.get(attrs, :deadline_at)) do
      {:ok,
       row
       |> transition_to("supersede_requested")
       |> Map.merge(%{
         control_generation: row.control_generation + 1,
         control_attempt_sequence: 1,
         current_attempt_ref: attempt_ref,
         generation_ref: generation_ref,
         external_operation_ref: external_ref,
         deadline_at: Map.get(attrs, :deadline_at, row.deadline_at),
         terminal_receipt_ref: nil,
         last_control_error: nil,
         reconcile_owner: nil,
         reconcile_lease_expires_at: nil,
         next_reconcile_at: nil
       })}
    end
  end

  defp transition(%{control_state: "supersede_requested"} = row, :supersede_ack, _attrs, _now),
    do: {:ok, transition_to(row, "running")}

  defp transition(%{control_state: state} = row, :deadline, _attrs, now)
       when state not in @terminal_states do
    cond do
      is_nil(row.deadline_at) ->
        {:error, :deadline_not_configured}

      DateTime.compare(as_datetime(row.deadline_at), now) == :gt ->
        {:error, :deadline_not_due}

      state in ~w(outcome_unknown reconciling) ->
        {:ok,
         row
         |> transition_to("operator_required")
         |> Map.put(:last_control_error, "deadline_expired_during_ambiguous_effect")}

      true ->
        {:ok,
         row
         |> transition_to("cancel_requested")
         |> Map.put(:last_control_error, "deadline_expired")}
    end
  end

  defp transition(%{control_state: state} = row, :outcome_unknown, attrs, _now)
       when state in ~w(
              running pause_requested resume_requested cancel_requested
              retry_requested supersede_requested
            ) do
    external_ref = Map.get(attrs, :external_operation_ref, row.external_operation_ref)

    if present_ref?(external_ref) do
      {:ok,
       row
       |> transition_to("outcome_unknown")
       |> Map.merge(%{
         external_operation_ref: external_ref,
         last_control_error: Map.get(attrs, :error_ref, "error://mezzanine/outcome-unknown"),
         next_reconcile_at: nil
       })}
    else
      {:error, :external_operation_ref_required}
    end
  end

  defp transition(%{control_state: state} = row, :restart_reconcile, attrs, now)
       when state in ~w(running outcome_unknown reconciling) do
    with true <- present_ref?(row.external_operation_ref),
         {:ok, owner} <- required_ref(attrs, :reconcile_owner),
         lease_seconds when is_integer(lease_seconds) and lease_seconds > 0 <-
           Map.get(attrs, :reconcile_lease_seconds, 30) do
      {:ok,
       row
       |> transition_to("reconciling")
       |> Map.merge(%{
         fence_epoch: row.fence_epoch + 1,
         reconciliation_attempts: row.reconciliation_attempts + 1,
         reconcile_owner: owner,
         reconcile_lease_expires_at: DateTime.add(now, lease_seconds, :second),
         next_reconcile_at: now,
         last_control_error:
           if(state == "running",
             do: "owner_lost_outcome_unknown",
             else: row.last_control_error
           )
       })}
    else
      false -> {:error, :external_operation_ref_required}
      _invalid -> {:error, :invalid_reconciliation_lease}
    end
  end

  defp transition(%{control_state: "reconciling"} = row, :reconcile_active, attrs, _now) do
    with :ok <- matching_fence(row, attrs) do
      {:ok, row |> transition_to("running") |> clear_reconciliation()}
    end
  end

  defp transition(%{control_state: "reconciling"} = row, :reconcile_completed, attrs, _now) do
    with :ok <- matching_fence(row, attrs),
         {:ok, receipt_ref} <- required_ref(attrs, :receipt_ref) do
      {:ok,
       row
       |> transition_to("completed")
       |> Map.put(:terminal_receipt_ref, receipt_ref)
       |> clear_reconciliation()}
    end
  end

  defp transition(%{control_state: "reconciling"} = row, :reconcile_failed, attrs, _now) do
    with :ok <- matching_fence(row, attrs),
         {:ok, receipt_ref} <- required_ref(attrs, :receipt_ref) do
      {:ok,
       row
       |> transition_to("failed")
       |> Map.put(:terminal_receipt_ref, receipt_ref)
       |> clear_reconciliation()}
    end
  end

  defp transition(%{control_state: "reconciling"} = row, :reconcile_not_found, attrs, _now) do
    with :ok <- matching_fence(row, attrs) do
      {:ok,
       row
       |> transition_to("operator_required")
       |> Map.put(:last_control_error, "external_operation_not_found")
       |> clear_reconciliation()}
    end
  end

  defp transition(%{control_state: "reconciling"} = row, :reconcile_unavailable, attrs, now) do
    with :ok <- matching_fence(row, attrs),
         {:ok, error} <- required_ref(attrs, :error_ref) do
      {:ok,
       row
       |> transition_to("reconciling")
       |> Map.merge(%{
         last_control_error: error,
         next_reconcile_at: Map.get(attrs, :next_reconcile_at, DateTime.add(now, 30, :second))
       })}
    end
  end

  defp transition(%{control_state: "operator_required"} = row, :operator_fail, attrs, _now) do
    with {:ok, receipt_ref} <- required_ref(attrs, :receipt_ref) do
      {:ok,
       row
       |> transition_to("failed")
       |> Map.put(:terminal_receipt_ref, receipt_ref)}
    end
  end

  defp transition(row, action, _attrs, _now),
    do: {:error, {:invalid_control_transition, row.control_state, action}}

  defp maybe_build_outbox(
         row,
         context,
         attrs,
         transition,
         action,
         sequence,
         event_ref,
         now,
         opts
       ) do
    signal_action =
      case action do
        action when action in [:pause, :resume, :cancel, :retry, :supersede] -> action
        :deadline when transition.control_state == "cancel_requested" -> :deadline
        _other -> nil
      end

    if signal_action do
      build_outbox!(
        row,
        context,
        attrs,
        transition,
        signal_action,
        sequence,
        event_ref,
        now,
        opts
      )
    end
  end

  defp build_outbox!(
         row,
         context,
         attrs,
         transition,
         signal_action,
         sequence,
         event_ref,
         now,
         opts
       ) do
    workflow_ref = workflow_ref!(row.run_ref, opts)
    {:ok, registry} = ControlSignalProtocol.signal_for_action(signal_action)
    token = digest_token({row.run_ref, attrs.command_ref})
    signal_id = "signal://mezzanine/control/#{token}"
    outbox_ref = "outbox://mezzanine/control/#{token}"
    payload_ref = Map.get(attrs, :payload_ref, event_ref)

    signal_attrs = %{
      tenant_ref: row.tenant_ref,
      installation_ref: row.installation_ref,
      principal_ref: context.actor_ref,
      operator_ref: Map.get(context, :operator_ref, context.actor_ref),
      resource_ref: row.run_ref,
      workflow_id: workflow_ref,
      signal_id: signal_id,
      signal_name: registry.signal_name,
      signal_version: registry.signal_version,
      signal_sequence: sequence,
      signal_effect: registry.signal_effect,
      authority_packet_ref: context.authority_ref,
      permission_decision_ref: context.permission_decision_ref,
      idempotency_key: attrs.idempotency_key,
      trace_id: context.trace_ref,
      correlation_id: context.correlation_ref,
      release_manifest_ref: @release_manifest_ref,
      acknowledgement_ttl_ms: Map.get(attrs, :acknowledgement_ttl_ms, 30_000),
      reason: Map.get(attrs, :reason),
      payload_hash: payload_digest(transition_payload(transition)),
      payload_ref: payload_ref
    }

    case ControlSignalProtocol.build_signal(signal_attrs) do
      {:ok, signal} ->
        payload =
          signal
          |> Map.from_struct()
          |> Map.merge(%{
            "run_ref" => row.run_ref,
            "command_ref" => attrs.command_ref,
            "control_row_version" => transition.control_row_version + 1,
            "attempt_ref" => transition.current_attempt_ref,
            "generation_ref" => transition.generation_ref,
            "external_operation_ref" => transition.external_operation_ref,
            "deadline_at" => iso8601(transition.deadline_at)
          })
          |> json_map()

        %{
          outbox_ref: outbox_ref,
          run_ref: row.run_ref,
          tenant_ref: row.tenant_ref,
          command_ref: attrs.command_ref,
          workflow_ref: workflow_ref,
          signal_id: signal_id,
          signal_name: registry.signal_name,
          signal_version: registry.signal_version,
          signal_payload: payload,
          payload_digest: payload_digest(payload),
          authority_ref: context.authority_ref,
          idempotency_key: attrs.idempotency_key,
          state: "queued",
          available_at: now
        }

      {:error, reason} ->
        repo(opts).rollback(reason)
    end
  end

  defp persist_projection!(row, updated, opts) do
    projection =
      row.projection
      |> Map.put("control", json_map(public_row(updated)))

    case SQL.query!(
           repo(opts),
           """
           UPDATE agent_run_projections
           SET control_state = $2,
               control_generation = $3,
               control_attempt_sequence = $4,
               control_sequence = $5,
               control_row_version = $6,
               current_attempt_ref = $7,
               generation_ref = $8,
               external_operation_ref = $9,
               deadline_at = $10,
               fence_epoch = $11,
               reconciliation_attempts = $12,
               reconcile_owner = $13,
               reconcile_lease_expires_at = $14,
               next_reconcile_at = $15,
               terminal_receipt_ref = $16,
               last_control_error = $17,
               control_updated_at = $18,
               projection = $19,
               run_revision = run_revision + 1,
               updated_at = $18
           WHERE run_ref = $1 AND control_row_version = $20
           RETURNING run_ref
           """,
           [
             row.run_ref,
             updated.control_state,
             updated.control_generation,
             updated.control_attempt_sequence,
             updated.control_sequence,
             updated.control_row_version,
             updated.current_attempt_ref,
             updated.generation_ref,
             updated.external_operation_ref,
             updated.deadline_at,
             updated.fence_epoch,
             updated.reconciliation_attempts,
             updated.reconcile_owner,
             updated.reconcile_lease_expires_at,
             updated.next_reconcile_at,
             updated.terminal_receipt_ref,
             updated.last_control_error,
             updated.control_updated_at,
             projection,
             row.control_row_version
           ]
         ).rows do
      [[_run_ref]] -> :ok
      [] -> repo(opts).rollback({:stale_control_version, row.control_row_version})
    end
  end

  defp persist_event!(row, updated, attrs, action, event_ref, digest, now, opts) do
    SQL.query!(
      repo(opts),
      """
      INSERT INTO agent_run_control_events
        (event_ref, run_ref, tenant_id, sequence, command_ref, event_type,
         from_state, to_state, attempt_ref, generation_ref, external_operation_ref,
         fence_epoch, payload_digest, metadata, recorded_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
      """,
      [
        event_ref,
        row.run_ref,
        row.tenant_ref,
        updated.control_sequence,
        attrs.command_ref,
        Atom.to_string(action),
        row.control_state,
        updated.control_state,
        updated.current_attempt_ref,
        updated.generation_ref,
        updated.external_operation_ref,
        updated.fence_epoch,
        digest,
        event_metadata(attrs),
        now
      ]
    )
  end

  defp persist_timeline_event!(row, context, attrs, control_event_ref, digest, now, opts) do
    case SQL.query!(
           repo(opts),
           """
           SELECT run_id, latest_event_ref, event_sequence, projection
           FROM agent_run_projections
           WHERE run_ref = $1
           FOR UPDATE
           """,
           [row.run_ref]
         ).rows do
      [[run_id, latest_event_ref, event_sequence, projection]] ->
        event =
          Event.new!(
            event_ref:
              "event://mezzanine/control-timeline/#{digest_token({control_event_ref, digest})}",
            run_ref: row.run_ref,
            tenant_ref: row.tenant_ref,
            event_type: "run_control_updated",
            event_version: 1,
            sequence: event_sequence + 1,
            command_ref: attrs.command_ref,
            causation_ref: latest_event_ref,
            correlation_ref: context.correlation_ref,
            payload_ref: Map.get(attrs, :payload_ref, control_event_ref),
            payload_digest: digest,
            recorded_at: now,
            row_version: 1
          )

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
            run_id,
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

        require_timeline_update!(
          """
          UPDATE agent_run_cursors
          SET last_event_ref = $2, sequence = $3, row_version = row_version + 1,
              updated_at = $4
          WHERE run_ref = $1 AND last_event_ref = $5 AND sequence = $6
          RETURNING run_ref
          """,
          [row.run_ref, event.event_ref, event.sequence, now, latest_event_ref, event_sequence],
          :run_cursor_conflict,
          opts
        )

        require_timeline_update!(
          """
          UPDATE agent_run_projections
          SET latest_event_ref = $2, event_sequence = $3,
              projection = $4, run_revision = run_revision + 1, updated_at = $5
          WHERE run_ref = $1 AND event_sequence = $6
          RETURNING run_ref
          """,
          [row.run_ref, event.event_ref, event.sequence, projection, now, event_sequence],
          :run_projection_state_conflict,
          opts
        )

      [] ->
        repo(opts).rollback(:run_projection_not_found)
    end
  end

  defp require_timeline_update!(sql, params, reason, opts) do
    case SQL.query!(repo(opts), sql, params).rows do
      [[_identity]] -> :ok
      [] -> repo(opts).rollback(reason)
    end
  end

  defp persist_outbox!(outbox, now, opts) do
    SQL.query!(
      repo(opts),
      """
      INSERT INTO agent_control_signal_outbox
        (outbox_ref, run_ref, tenant_id, command_ref, workflow_ref, signal_id,
         signal_name, signal_version, signal_payload, payload_digest, authority_ref,
         idempotency_key, state, attempt, available_at, dispatch_fence, row_version,
         inserted_at, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,0,$14,0,1,$15,$15)
      """,
      [
        outbox.outbox_ref,
        outbox.run_ref,
        outbox.tenant_ref,
        outbox.command_ref,
        outbox.workflow_ref,
        outbox.signal_id,
        outbox.signal_name,
        outbox.signal_version,
        outbox.signal_payload,
        outbox.payload_digest,
        outbox.authority_ref,
        outbox.idempotency_key,
        outbox.state,
        outbox.available_at,
        now
      ]
    )
  end

  defp persist_command!(
         row,
         attrs,
         action,
         expected_version,
         digest,
         result,
         now,
         opts
       ) do
    SQL.query!(
      repo(opts),
      """
      INSERT INTO agent_run_control_commands
        (command_ref, run_ref, tenant_id, idempotency_key, request_digest,
         action, expected_row_version, result_row_version, result, recorded_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      """,
      [
        attrs.command_ref,
        row.run_ref,
        row.tenant_ref,
        attrs.idempotency_key,
        digest,
        Atom.to_string(action),
        expected_version,
        result.control_row_version,
        json_map(result),
        now
      ]
    )
  end

  defp locked_row!(run_ref, opts) do
    case SQL.query!(
           repo(opts),
           projection_select() <> " WHERE projection.run_ref = $1 FOR UPDATE OF projection",
           [run_ref]
         ).rows do
      [row] -> row_from_result(row)
      [] -> repo(opts).rollback(:run_not_found)
    end
  end

  defp projection_select do
    """
    SELECT projection.run_ref, projection.tenant_id, command.installation_ref,
           projection.subject_ref, projection.status, projection.control_state,
           projection.control_generation, projection.control_attempt_sequence,
           projection.control_sequence, projection.control_row_version,
           projection.current_attempt_ref, projection.generation_ref,
           projection.external_operation_ref, projection.deadline_at,
           projection.fence_epoch, projection.reconciliation_attempts,
           projection.reconcile_owner, projection.reconcile_lease_expires_at,
           projection.next_reconcile_at, projection.terminal_receipt_ref,
           projection.last_control_error, projection.control_updated_at,
           projection.updated_at, projection.projection
    FROM agent_run_projections AS projection
    JOIN agent_run_commands AS command ON command.run_ref = projection.run_ref
    """
  end

  defp existing_command(row, idempotency_key, opts) do
    case SQL.query!(
           repo(opts),
           """
           SELECT request_digest, action, result
           FROM agent_run_control_commands
           WHERE run_ref = $1 AND idempotency_key = $2
           """,
           [row.run_ref, idempotency_key]
         ).rows do
      [[digest, action, result]] ->
        {:ok, %{request_digest: digest, action: action, result: atomize_known_result(result)}}

      [] ->
        :error
    end
  end

  defp workflow_ref!(run_ref, opts) do
    case SQL.query!(
           repo(opts),
           """
           SELECT workflow_ref
           FROM agent_workflow_outbox
           WHERE run_ref = $1
           ORDER BY inserted_at DESC
           LIMIT 1
           """,
           [run_ref]
         ).rows do
      [[workflow_ref]] -> workflow_ref
      [] -> repo(opts).rollback(:workflow_ref_not_found)
    end
  end

  defp database_now!(opts) do
    case SQL.query!(repo(opts), "SELECT clock_timestamp()", []).rows do
      [[now]] -> as_datetime(now)
    end
  end

  defp ensure_tenant!(row, context, opts) do
    if row.tenant_ref == context.tenant_ref,
      do: :ok,
      else: repo(opts).rollback(:cross_tenant_control_denied)
  end

  defp validate_context(context) do
    required = [
      :tenant_ref,
      :actor_ref,
      :authority_ref,
      :permission_decision_ref,
      :trace_ref,
      :correlation_ref
    ]

    if Enum.all?(required, &present_ref?(Map.get(context, &1))),
      do: :ok,
      else: {:error, :invalid_control_context}
  end

  defp validate_command_attrs(attrs) do
    if present_ref?(Map.get(attrs, :command_ref)) and
         present_string?(Map.get(attrs, :idempotency_key)) do
      :ok
    else
      {:error, :invalid_control_command_identity}
    end
  end

  defp matching_fence(row, attrs) do
    if Map.get(attrs, :expected_fence_epoch) == row.fence_epoch,
      do: :ok,
      else: {:error, {:stale_fence_epoch, row.fence_epoch}}
  end

  defp required_ref(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {key, :required}}
    end
  end

  defp new_ref(attrs, key, current) do
    with {:ok, value} <- required_ref(attrs, key),
         true <- value != current do
      {:ok, value}
    else
      false -> {:error, {key, :must_change}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp valid_deadline(nil), do: :ok
  defp valid_deadline(%DateTime{}), do: :ok
  defp valid_deadline(_other), do: {:error, :invalid_deadline}

  defp transition_to(row, state), do: Map.put(row, :control_state, state)

  defp clear_reconciliation(row) do
    Map.merge(row, %{
      reconcile_owner: nil,
      reconcile_lease_expires_at: nil,
      next_reconcile_at: nil,
      last_control_error: nil
    })
  end

  defp request_digest(context, run_ref, action, expected_version, attrs) do
    payload_digest(%{
      context: context,
      run_ref: run_ref,
      action: action,
      expected_version: expected_version,
      attrs: attrs
    })
  end

  defp transition_payload(row) do
    Map.take(row, [
      :run_ref,
      :control_state,
      :control_generation,
      :control_attempt_sequence,
      :current_attempt_ref,
      :generation_ref,
      :external_operation_ref,
      :deadline_at,
      :fence_epoch
    ])
  end

  defp payload_digest(term) do
    "sha256:" <>
      (term
       |> :erlang.term_to_binary()
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower))
  end

  defp error_ref(reason), do: "error://mezzanine/recovery/#{digest_token(reason)}"

  defp control_event_ref(run_ref, sequence) do
    "event://mezzanine/control/#{digest_token(run_ref)}/#{sequence}"
  end

  defp digest_token(term) do
    term
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp system_context(tenant_ref, installation_ref, class, run_ref, epoch) do
    %{
      tenant_ref: tenant_ref,
      installation_ref: installation_ref,
      actor_ref: "actor://mezzanine/system/#{class}",
      operator_ref: "operator://mezzanine/system/#{class}",
      authority_ref: "authority://mezzanine/system/#{class}",
      permission_decision_ref: "decision://mezzanine/system/#{class}/#{epoch}",
      trace_ref: "trace://mezzanine/#{class}/#{digest_token(run_ref)}",
      correlation_ref: "correlation://mezzanine/#{class}/#{epoch}"
    }
  end

  defp public_row(row) do
    Map.take(row, @public_row_keys)
    |> Map.new(fn
      {key, %NaiveDateTime{} = value} -> {key, as_datetime(value)}
      pair -> pair
    end)
  end

  defp row_from_result(row), do: zip_result(row, @row_columns)
  defp outbox_from_result(row), do: zip_result(row, @outbox_columns)

  defp event_from_result([
         event_ref,
         run_ref,
         tenant_ref,
         sequence,
         command_ref,
         event_type,
         from_state,
         to_state,
         attempt_ref,
         generation_ref,
         external_operation_ref,
         fence_epoch,
         payload_digest,
         metadata,
         recorded_at
       ]) do
    %{
      event_ref: event_ref,
      run_ref: run_ref,
      tenant_ref: tenant_ref,
      sequence: sequence,
      command_ref: command_ref,
      event_type: event_type,
      from_state: from_state,
      to_state: to_state,
      attempt_ref: attempt_ref,
      generation_ref: generation_ref,
      external_operation_ref: external_operation_ref,
      fence_epoch: fence_epoch,
      payload_digest: payload_digest,
      metadata: metadata,
      recorded_at: as_datetime(recorded_at)
    }
  end

  defp zip_result(row, columns) do
    row
    |> Enum.zip(columns)
    |> Map.new(fn
      {%NaiveDateTime{} = value, key} -> {key, as_datetime(value)}
      {value, key} -> {key, value}
    end)
  end

  defp event_metadata(attrs) do
    attrs
    |> Map.drop([
      :idempotency_key,
      :payload_ref,
      :acknowledgement_ttl_ms
    ])
    |> json_map()
  end

  defp atomize_known_result(result) do
    known =
      @public_row_keys
      |> Kernel.++([:command_ref, :event_ref, :outbox_ref, :idempotent_replay?])
      |> Map.new(&{Atom.to_string(&1), &1})

    result
    |> Map.new(fn {key, value} -> {Map.get(known, key, key), value} end)
    |> Map.put(:idempotent_replay?, true)
  end

  defp json_map(map) do
    Map.new(map, fn {key, value} ->
      {to_string(key), json_value(value)}
    end)
  end

  defp json_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp json_value(%NaiveDateTime{} = value), do: value |> as_datetime() |> DateTime.to_iso8601()
  defp json_value(%MapSet{} = value), do: value |> MapSet.to_list() |> Enum.map(&json_value/1)
  defp json_value(%_{} = value), do: value |> Map.from_struct() |> json_map()
  defp json_value(value) when is_map(value), do: json_map(value)
  defp json_value(value) when is_list(value), do: Enum.map(value, &json_value/1)
  defp json_value(value) when is_atom(value), do: Atom.to_string(value)

  defp json_value(value) when is_tuple(value),
    do: value |> Tuple.to_list() |> Enum.map(&json_value/1)

  defp json_value(value), do: value

  defp iso8601(nil), do: nil
  defp iso8601(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp as_datetime(%DateTime{} = value), do: value
  defp as_datetime(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")

  defp present_ref?(value), do: present_string?(value)
  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize()

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {normalize_key(key), value}
      pair -> pair
    end)
  end

  defp normalize_key(key) do
    case key do
      "tenant_ref" -> :tenant_ref
      "installation_ref" -> :installation_ref
      "actor_ref" -> :actor_ref
      "operator_ref" -> :operator_ref
      "authority_ref" -> :authority_ref
      "permission_decision_ref" -> :permission_decision_ref
      "trace_ref" -> :trace_ref
      "correlation_ref" -> :correlation_ref
      "command_ref" -> :command_ref
      "idempotency_key" -> :idempotency_key
      "attempt_ref" -> :attempt_ref
      "generation_ref" -> :generation_ref
      "external_operation_ref" -> :external_operation_ref
      "deadline_at" -> :deadline_at
      "receipt_ref" -> :receipt_ref
      "reason" -> :reason
      "error_ref" -> :error_ref
      "expected_fence_epoch" -> :expected_fence_epoch
      "next_reconcile_at" -> :next_reconcile_at
      "reconcile_owner" -> :reconcile_owner
      "reconcile_lease_seconds" -> :reconcile_lease_seconds
      "acknowledgement_ttl_ms" -> :acknowledgement_ttl_ms
      "payload_ref" -> :payload_ref
      other -> other
    end
  end

  defp collect_successes(results) do
    Enum.reduce_while(results, {:ok, []}, fn
      {:ok, result}, {:ok, acc} -> {:cont, {:ok, [result | acc]}}
      {:error, {:stale_control_version, _current}}, {:ok, acc} -> {:cont, {:ok, acc}}
      {:error, reason}, _acc -> {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp transaction(repo, fun) do
    case repo.transaction(fun) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, reason}
    end
  rescue
    _error in [DBConnection.ConnectionError, Ecto.ConstraintError, Postgrex.Error] ->
      {:error, :postgres_write_failed}
  catch
    :exit, _reason -> {:error, :postgres_unavailable}
  end

  defp repo(opts), do: Keyword.get(opts, :repo, Mezzanine.OpsDomain.Repo)
end
