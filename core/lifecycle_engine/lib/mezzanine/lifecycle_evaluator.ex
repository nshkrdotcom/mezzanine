defmodule Mezzanine.LifecycleEvaluator do
  @moduledoc """
  Durable lifecycle coordinator for explicit `{:execution_requested, recipe_ref}`
  transitions.

  This module owns the same-database transaction that moves subject truth to the
  next lifecycle state, seeds a queued `ExecutionRecord`, and records the
  Temporal workflow handoff owned by `Mezzanine.WorkflowRuntime`.
  """

  alias Ecto.{Adapters.SQL, Multi}
  alias Mezzanine.Audit.AuditAppend
  alias Mezzanine.Execution.{DispatchState, PayloadBoundary}
  alias Mezzanine.Execution.Repo
  alias Mezzanine.Lifecycle.DispatchEnvelopeRefValidator
  alias Mezzanine.Lifecycle.Evaluator, as: PackEvaluator
  alias Mezzanine.Lifecycle.SubjectSnapshot
  alias Mezzanine.Pack.{CompiledPack, ExecutionRecipeSpec, Serializer}
  alias Mezzanine.WorkflowStartOutboxPayload

  @dialyzer [
    {:nowarn_function, advance_multi: 2},
    {:nowarn_function, execution_plan_multi: 5},
    {:nowarn_function, dispatch_multi: 2},
    {:nowarn_function, cycle_bound_multi: 3},
    {:nowarn_function, build_result_multi: 1}
  ]

  @active_dispatch_states DispatchState.active_state_strings()
  @blocked_on_cycle_state "blocked_on_cycle"
  @default_max_supersession_depth 8
  @workflow_start_outbox_worker "Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker"
  @workflow_start_outbox_queue :workflow_start_outbox
  @workflow_start_release_manifest_ref "phase4-v6-milestone31-temporal-cutover"
  @workflow_start_required_fields [
    :outbox_id,
    :tenant_ref,
    :installation_ref,
    :principal_ref,
    :resource_ref,
    :command_receipt_ref,
    :command_id,
    :workflow_type,
    :workflow_id,
    :workflow_version,
    :workflow_input_version,
    :workflow_input_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :dedupe_scope,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :payload_hash,
    :dispatch_state
  ]
  @workflow_start_correlation_fields [
    :canonical_idempotency_key,
    :client_retry_key,
    :platform_envelope_idempotency_key,
    :causation_id,
    :idempotency_correlation
  ]
  @workflow_start_unique [
    keys: [:workflow_id, :idempotency_key],
    states: [:available, :scheduled, :executing, :retryable],
    period: :infinity
  ]
  @supersession_reasons [
    :retry_transient,
    :retry_semantic,
    :operator_replan,
    :pack_revision_change,
    :manual_retry
  ]
  @supersession_reason_lookup Map.new(@supersession_reasons, &{Atom.to_string(&1), &1})
  @default_actor_ref %{kind: :lifecycle_evaluator}

  @subject_lock_sql """
  SELECT pg_advisory_xact_lock(hashtext('mezzanine.subject:' || $1))
  """

  @load_subject_sql """
  SELECT id, installation_id, subject_kind, lifecycle_state, payload, row_version
  FROM subject_records
  WHERE id = $1::uuid
  FOR UPDATE
  """

  @load_installation_sql """
  SELECT i.id,
         i.tenant_id,
         i.compiled_pack_revision,
         i.binding_config,
         i.status,
         pr.compiled_manifest
  FROM installations AS i
  INNER JOIN pack_registrations AS pr ON pr.id = i.pack_registration_id
  WHERE i.id = $1::uuid
  """

  @load_trace_sql """
  SELECT trace_id
  FROM audit_facts
  WHERE subject_id = $1
  ORDER BY occurred_at DESC, inserted_at DESC
  LIMIT 1
  """

  @active_execution_sql """
  SELECT 1
  FROM execution_records
  WHERE subject_id = $1::uuid
    AND dispatch_state = ANY($2)
  LIMIT 1
  """

  @load_superseded_execution_sql """
  SELECT id, installation_id, subject_id, supersession_depth
  FROM execution_records
  WHERE id = $1::uuid
  LIMIT 1
  """

  @update_subject_sql """
  UPDATE subject_records
  SET lifecycle_state = $2,
      row_version = row_version + 1,
      updated_at = $3
  WHERE id = $1::uuid
    AND row_version = $4
  RETURNING row_version
  """

  @insert_execution_sql """
  INSERT INTO execution_records (
    id,
    tenant_id,
    installation_id,
    subject_id,
    barrier_id,
    recipe_ref,
    compiled_pack_revision,
    binding_snapshot,
    dispatch_envelope,
    intent_snapshot,
    submission_dedupe_key,
    trace_id,
    causation_id,
    dispatch_state,
    dispatch_attempt_count,
    next_dispatch_at,
    submission_ref,
    lower_receipt,
    last_dispatch_error_payload,
    supersedes_execution_id,
    supersession_reason,
    supersession_depth,
    row_version,
    inserted_at,
    updated_at
  )
  VALUES (
    $1::uuid,
    $2,
    $3,
    $4::uuid,
    $5::uuid,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $13,
    'queued',
    0,
    $14,
    $15,
    $16,
    $17,
    $18::uuid,
    $19,
    $20,
    1,
    $14,
    $14
  )
  RETURNING id
  """

  @insert_workflow_start_outbox_sql """
  INSERT INTO workflow_start_outbox (
    outbox_id,
    tenant_ref,
    installation_ref,
    workspace_ref,
    project_ref,
    environment_ref,
    principal_ref,
    system_actor_ref,
    resource_ref,
    command_envelope_ref,
    command_receipt_ref,
    command_id,
    workflow_type,
    workflow_id,
    workflow_version,
    workflow_input_version,
    workflow_input_ref,
    authority_packet_ref,
    permission_decision_ref,
    idempotency_key,
    dedupe_scope,
    trace_id,
    correlation_id,
    release_manifest_ref,
    payload_hash,
    payload_ref,
    dispatch_state,
    retry_count,
    last_error_class,
    available_at,
    row_version,
    inserted_at,
    updated_at
  )
  VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
    1, $31, $31
  )
  RETURNING outbox_id
  """

  @upsert_lineage_sql """
  INSERT INTO execution_lineage_records (
    id,
    trace_id,
    causation_id,
    tenant_id,
    installation_id,
    subject_id,
    execution_id,
    citadel_request_id,
    citadel_submission_id,
    ji_submission_key,
    lower_run_id,
    lower_attempt_id,
    artifact_refs,
    inserted_at,
    updated_at
  )
  VALUES (
    gen_random_uuid(),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    $7,
    $8,
    $8
  )
  ON CONFLICT (execution_id) DO UPDATE
  SET trace_id = EXCLUDED.trace_id,
      causation_id = EXCLUDED.causation_id,
      tenant_id = EXCLUDED.tenant_id,
      installation_id = EXCLUDED.installation_id,
      subject_id = EXCLUDED.subject_id,
      artifact_refs = EXCLUDED.artifact_refs,
      updated_at = EXCLUDED.updated_at
  """

  @type noop_reason ::
          :no_execution_requested_transition
          | :no_matching_trigger_transition
          | :guard_blocked
          | :active_execution_present

  @type supersession_reason ::
          :retry_transient
          | :retry_semantic
          | :operator_replan
          | :pack_revision_change
          | :manual_retry

  @type supersession_opts :: %{
          supersedes_execution_id: Ecto.UUID.t(),
          supersession_reason: supersession_reason()
        }

  @type trigger_key ::
          :auto
          | {:execution_requested, String.t()}
          | {:execution_completed, String.t()}
          | {:execution_failed, String.t()}
          | {:execution_failed, String.t(), atom()}
          | {:join_completed, String.t()}

  @type advance_result ::
          {:ok, %{action: :noop, reason: noop_reason(), subject_id: Ecto.UUID.t()}}
          | {:ok,
             %{
               action: :advanced_state,
               subject_id: Ecto.UUID.t(),
               from_state: String.t(),
               to_state: String.t(),
               trigger: map(),
               trace_id: String.t()
             }}
          | {:ok,
             %{
               action: :queued_execution,
               subject_id: Ecto.UUID.t(),
               execution_id: Ecto.UUID.t(),
               recipe_ref: String.t(),
               to_state: String.t(),
               submission_dedupe_key: String.t(),
               trace_id: String.t()
             }}
          | {:error, term()}

  @spec advance(Ecto.UUID.t(), keyword()) :: advance_result()
  def advance(subject_id, opts \\ []) when is_binary(subject_id) and is_list(opts) do
    with {:ok, multi} <- advance_transaction(subject_id, opts) do
      case Repo.transaction(multi) do
        {:ok, %{advance_result: result}} -> {:ok, result}
        {:error, _step, error, _changes} -> {:error, error}
      end
    end
  end

  @spec advance_transaction(Ecto.UUID.t(), keyword()) :: {:ok, Ecto.Multi.t()} | {:error, term()}
  def advance_transaction(subject_id, opts \\ []) when is_binary(subject_id) and is_list(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now())
    actor_ref = normalize_map(Keyword.get(opts, :actor_ref, @default_actor_ref))
    trace_id = Keyword.get(opts, :trace_id)
    execution_id = Keyword.get(opts, :execution_id)
    trigger = Keyword.get(opts, :trigger, :auto)
    causation_id = Keyword.get(opts, :causation_id, default_causation_id(subject_id, now))

    expected_installation_revision =
      Keyword.get(opts, :expected_installation_revision) ||
        Keyword.get(opts, :installation_revision)

    with {:ok, supersession_opts} <- normalize_supersession_opts(opts) do
      {:ok,
       advance_multi(subject_id, %{
         now: now,
         actor_ref: actor_ref,
         explicit_trace_id: trace_id,
         execution_id: execution_id,
         trigger: trigger,
         causation_id: causation_id,
         supersession_opts: supersession_opts,
         expected_installation_revision: expected_installation_revision
       })}
    end
  end

  @spec advance_multi(Ecto.UUID.t(), map()) :: Ecto.Multi.t()
  defp advance_multi(subject_id, context) do
    Multi.new()
    |> Multi.run(:subject_lock, fn repo, _changes ->
      case execute(repo, @subject_lock_sql, [subject_id]) do
        :ok -> {:ok, :locked}
        {:error, error} -> {:error, error}
      end
    end)
    |> Multi.run(:subject, fn repo, _changes ->
      fetch_subject_for_update(repo, subject_id)
    end)
    |> Multi.run(:installation, fn repo, %{subject: subject} ->
      fetch_installation(repo, subject.installation_id)
    end)
    |> Multi.run(:installation_revision_gate, fn _repo, %{installation: installation} ->
      ensure_expected_installation_revision(installation, context.expected_installation_revision)
    end)
    |> Multi.run(:supersession_context, fn repo,
                                           %{subject: subject, installation: installation} ->
      resolve_supersession_context(repo, subject, installation, context.supersession_opts)
    end)
    |> Multi.run(:compiled_pack, fn _repo, %{installation: installation} ->
      deserialize_compiled_pack(installation.compiled_manifest)
    end)
    |> Multi.run(:trace_id, fn repo, %{subject: subject} ->
      resolve_trace_id(repo, subject.id, context.explicit_trace_id)
    end)
    |> Multi.run(:plan, fn _repo, %{compiled_pack: compiled_pack, subject: subject} ->
      build_plan(compiled_pack, subject, context.trigger)
    end)
    |> Multi.merge(fn changes ->
      execution_plan_multi(
        changes,
        context.now,
        context.actor_ref,
        context.execution_id,
        context.causation_id
      )
    end)
  end

  @spec execution_plan_multi(map(), DateTime.t(), map(), Ecto.UUID.t() | nil, String.t()) ::
          Ecto.Multi.t()
  defp execution_plan_multi(
         %{plan: {:noop, reason}, subject: subject, trace_id: trace_id},
         _now,
         _actor_ref,
         _execution_id,
         _causation_id
       ) do
    build_result_multi(%{
      action: :noop,
      reason: reason,
      subject_id: subject.id,
      trace_id: trace_id
    })
  end

  defp execution_plan_multi(
         %{
           plan: {:state_transition, trigger, to_state},
           subject: subject,
           installation: installation,
           trace_id: trace_id
         },
         now,
         actor_ref,
         execution_id,
         causation_id
       ) do
    Multi.new()
    |> Multi.run(:subject_update, fn repo, _changes ->
      update_subject_lifecycle(repo, subject, to_state, now)
    end)
    |> Multi.run(:audit_lifecycle_advanced, fn repo, _changes ->
      append_audit_fact(
        repo,
        %{
          installation_id: installation.id,
          subject_id: subject.id,
          execution_id: execution_id,
          trace_id: trace_id,
          causation_id: causation_id,
          fact_kind: "lifecycle_advanced",
          actor_ref: actor_ref,
          payload: %{
            "from" => subject.lifecycle_state,
            "to" => to_state,
            "trigger" => serialize_trigger(trigger)
          }
        },
        now
      )
    end)
    |> Multi.run(:advance_result, fn _repo, _changes ->
      {:ok,
       %{
         action: :advanced_state,
         subject_id: subject.id,
         from_state: subject.lifecycle_state,
         to_state: to_state,
         trigger: serialize_trigger(trigger),
         trace_id: trace_id
       }}
    end)
  end

  defp execution_plan_multi(
         %{
           plan: {:dispatch, recipe_ref, to_state},
           subject: subject,
           installation: installation,
           compiled_pack: compiled_pack,
           supersession_context: supersession_context,
           trace_id: trace_id
         },
         now,
         actor_ref,
         _execution_id,
         causation_id
       ) do
    Multi.new()
    |> Multi.run(:active_execution_present, fn repo, _changes ->
      {:ok, active_execution_present?(repo, subject.id)}
    end)
    |> Multi.merge(fn changes ->
      if changes.active_execution_present do
        build_result_multi(%{
          action: :noop,
          reason: :active_execution_present,
          subject_id: subject.id,
          trace_id: trace_id
        })
      else
        dispatch_multi(
          %{
            subject: subject,
            installation: installation,
            compiled_pack: compiled_pack,
            recipe_ref: recipe_ref,
            to_state: to_state,
            supersession_context: supersession_context,
            trace_id: trace_id,
            causation_id: causation_id,
            actor_ref: actor_ref
          },
          now
        )
      end
    end)
  end

  @spec dispatch_multi(map(), DateTime.t()) :: Ecto.Multi.t()
  defp dispatch_multi(dispatch_context, now) do
    subject = dispatch_context.subject
    installation = dispatch_context.installation
    compiled_pack = dispatch_context.compiled_pack
    recipe_ref = dispatch_context.recipe_ref
    to_state = dispatch_context.to_state
    supersession_context = dispatch_context.supersession_context
    trace_id = dispatch_context.trace_id
    causation_id = dispatch_context.causation_id
    actor_ref = dispatch_context.actor_ref

    if cycle_bound_reached?(compiled_pack, supersession_context) do
      cycle_bound_multi(dispatch_context, now, max_supersession_depth(compiled_pack))
    else
      Multi.new()
      |> Multi.run(:recipe, fn _repo, _changes ->
        fetch_recipe(compiled_pack, recipe_ref)
      end)
      |> Multi.run(:binding_snapshot, fn _repo, %{recipe: recipe} ->
        binding_snapshot(installation.binding_config, recipe)
      end)
      |> Multi.run(:dispatch_envelope, fn _repo,
                                          %{recipe: recipe, binding_snapshot: binding_snapshot} ->
        {:ok, dispatch_envelope(recipe, binding_snapshot)}
      end)
      |> Multi.run(:intent_snapshot, fn _repo,
                                        %{
                                          recipe: recipe,
                                          binding_snapshot: binding_snapshot,
                                          dispatch_envelope: dispatch_envelope
                                        } ->
        {:ok, intent_snapshot(recipe, binding_snapshot, dispatch_envelope)}
      end)
      |> Multi.run(:submission_dedupe_key, fn _repo, _changes ->
        {:ok, Ecto.UUID.generate()}
      end)
      |> Multi.run(:subject_update, fn repo, _changes ->
        update_subject_lifecycle(repo, subject, to_state, now)
      end)
      |> Multi.run(:execution_id, fn repo, changes ->
        insert_execution_record(
          repo,
          subject,
          installation,
          %{
            recipe_ref: recipe_ref,
            binding_snapshot: changes.binding_snapshot,
            dispatch_envelope: changes.dispatch_envelope,
            intent_snapshot: changes.intent_snapshot,
            submission_dedupe_key: changes.submission_dedupe_key,
            trace_id: trace_id,
            causation_id: causation_id,
            supersedes_execution_id: supersession_id(supersession_context),
            supersession_reason: supersession_reason_value(supersession_context),
            supersession_depth: supersession_depth_value(supersession_context)
          },
          now
        )
      end)
      |> Multi.run(:workflow_handoff, fn repo, changes ->
        workflow_handoff(repo, dispatch_context, changes, now)
      end)
      |> then(fn multi ->
        Oban.insert(
          Mezzanine.Execution.Oban,
          multi,
          :workflow_start_dispatch_job,
          &workflow_start_job_changeset/1
        )
      end)
      |> Multi.run(:audit_lifecycle_advanced, fn repo, %{execution_id: execution_id} ->
        append_audit_fact(
          repo,
          %{
            installation_id: installation.id,
            subject_id: subject.id,
            execution_id: execution_id,
            trace_id: trace_id,
            causation_id: causation_id,
            fact_kind: "lifecycle_advanced",
            actor_ref: actor_ref,
            payload:
              lifecycle_advanced_payload(
                subject.lifecycle_state,
                to_state,
                recipe_ref,
                supersession_context
              )
          },
          now
        )
      end)
      |> Multi.run(:audit_execution_dispatched, fn repo, changes ->
        append_audit_fact(
          repo,
          %{
            installation_id: installation.id,
            subject_id: subject.id,
            execution_id: changes.execution_id,
            trace_id: trace_id,
            causation_id: causation_id,
            fact_kind: "execution_dispatched",
            actor_ref: actor_ref,
            payload:
              execution_dispatched_payload(
                recipe_ref,
                changes.submission_dedupe_key,
                changes.workflow_handoff,
                supersession_context
              )
          },
          now
        )
      end)
      |> Multi.run(:execution_lineage, fn repo, %{execution_id: execution_id} ->
        upsert_execution_lineage(
          repo,
          trace_id,
          causation_id,
          installation.tenant_id,
          installation.id,
          subject.id,
          execution_id,
          now
        )
      end)
      |> Multi.run(:advance_result, fn _repo, changes ->
        {:ok,
         %{
           action: :queued_execution,
           subject_id: subject.id,
           execution_id: changes.execution_id,
           recipe_ref: recipe_ref,
           to_state: to_state,
           submission_dedupe_key: changes.submission_dedupe_key,
           trace_id: trace_id,
           workflow_handoff: changes.workflow_handoff
         }}
      end)
    end
  end

  @spec build_result_multi(map()) :: Ecto.Multi.t()
  defp build_result_multi(result) do
    Multi.new()
    |> Multi.run(:advance_result, fn _repo, _changes -> {:ok, result} end)
  end

  @spec cycle_bound_multi(map(), DateTime.t(), pos_integer()) :: Ecto.Multi.t()
  defp cycle_bound_multi(dispatch_context, now, max_depth) do
    subject = dispatch_context.subject
    installation = dispatch_context.installation
    recipe_ref = dispatch_context.recipe_ref
    supersession_context = dispatch_context.supersession_context
    trace_id = dispatch_context.trace_id
    causation_id = dispatch_context.causation_id
    actor_ref = dispatch_context.actor_ref

    Multi.new()
    |> Multi.run(:subject_update, fn repo, _changes ->
      update_subject_lifecycle(repo, subject, @blocked_on_cycle_state, now)
    end)
    |> Multi.run(:audit_lifecycle_advanced, fn repo, _changes ->
      append_audit_fact(
        repo,
        %{
          installation_id: installation.id,
          subject_id: subject.id,
          execution_id: nil,
          trace_id: trace_id,
          causation_id: causation_id,
          fact_kind: "lifecycle_advanced",
          actor_ref: actor_ref,
          payload:
            lifecycle_advanced_payload(
              subject.lifecycle_state,
              @blocked_on_cycle_state,
              recipe_ref,
              supersession_context
            )
        },
        now
      )
    end)
    |> Multi.run(:audit_cycle_bound_reached, fn repo, _changes ->
      append_audit_fact(
        repo,
        %{
          installation_id: installation.id,
          subject_id: subject.id,
          execution_id: supersession_id(supersession_context),
          trace_id: trace_id,
          causation_id: causation_id,
          fact_kind: "cycle_bound_reached",
          actor_ref: actor_ref,
          payload: %{
            "attempted_recipe_ref" => recipe_ref,
            "attempted_state" => dispatch_context.to_state,
            "max_supersession_depth" => max_depth,
            "attempted_supersession_depth" => supersession_depth_value(supersession_context),
            "supersedes_execution_id" => supersession_id(supersession_context),
            "supersession_reason" => supersession_reason_value(supersession_context)
          }
        },
        now
      )
    end)
    |> Multi.run(:advance_result, fn _repo, _changes ->
      {:ok,
       %{
         action: :advanced_state,
         subject_id: subject.id,
         from_state: subject.lifecycle_state,
         to_state: @blocked_on_cycle_state,
         trigger: serialize_trigger({:execution_requested, recipe_ref}),
         trace_id: trace_id
       }}
    end)
  end

  defp build_plan(%CompiledPack{} = compiled_pack, subject, :auto) do
    build_execution_request_plan(compiled_pack, subject)
  end

  defp build_plan(%CompiledPack{} = compiled_pack, subject, trigger) do
    build_trigger_transition_plan(compiled_pack, subject, trigger)
  end

  defp build_execution_request_plan(%CompiledPack{} = compiled_pack, subject) do
    requested_transitions =
      compiled_pack
      |> CompiledPack.transitions_for(subject.subject_kind, subject.lifecycle_state)
      |> Enum.filter(fn {trigger, _transition} -> match?({:execution_requested, _}, trigger) end)

    case requested_transitions do
      [] ->
        {:ok, {:noop, :no_execution_requested_transition}}

      [{{:execution_requested, recipe_ref} = trigger, transition}] ->
        case PackEvaluator.can_transition?(compiled_pack, subject_snapshot(subject), trigger) do
          {:ok, _matched_transition} ->
            {:ok, {:dispatch, recipe_ref, transition.to}}

          {:error, :guard_failed} ->
            {:ok, {:noop, :guard_blocked}}

          {:error, :no_transition} ->
            {:ok, {:noop, :no_execution_requested_transition}}
        end

      requested ->
        {:error,
         {:ambiguous_execution_request,
          Enum.map(requested, fn {{:execution_requested, recipe_ref}, _transition} ->
            recipe_ref
          end)}}
    end
  end

  defp build_trigger_transition_plan(%CompiledPack{} = compiled_pack, subject, trigger) do
    case PackEvaluator.can_transition?(compiled_pack, subject_snapshot(subject), trigger) do
      {:ok, transition} ->
        {:ok, {:state_transition, trigger, transition.to}}

      {:error, :guard_failed} ->
        {:ok, {:noop, :guard_blocked}}

      {:error, :no_transition} ->
        {:ok, {:noop, :no_matching_trigger_transition}}
    end
  end

  defp fetch_subject_for_update(repo, subject_id) do
    case SQL.query(repo, @load_subject_sql, [dump_uuid!(subject_id)]) do
      {:ok, %{rows: [[id, installation_id, subject_kind, lifecycle_state, payload, row_version]]}} ->
        {:ok,
         %{
           id: load_uuid!(id),
           installation_id: installation_id,
           subject_kind: subject_kind,
           lifecycle_state: lifecycle_state,
           payload: payload || %{},
           row_version: row_version
         }}

      {:ok, %{rows: []}} ->
        {:error, {:subject_not_found, subject_id}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp fetch_installation(repo, installation_id) do
    case SQL.query(repo, @load_installation_sql, [dump_uuid!(installation_id)]) do
      {:ok,
       %{
         rows: [
           [id, tenant_id, compiled_pack_revision, binding_config, status, compiled_manifest]
         ]
       }} ->
        if status == "active" do
          {:ok,
           %{
             id: load_uuid!(id),
             tenant_id: tenant_id,
             compiled_pack_revision: compiled_pack_revision,
             binding_config: normalize_map(binding_config || %{}),
             compiled_manifest: compiled_manifest || %{}
           }}
        else
          {:error, {:installation_not_active, installation_id, status}}
        end

      {:ok, %{rows: []}} ->
        {:error, {:installation_not_found, installation_id}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp ensure_expected_installation_revision(_installation, nil), do: {:ok, :not_required}

  defp ensure_expected_installation_revision(installation, expected_revision)
       when is_integer(expected_revision) and expected_revision >= 0 do
    if installation.compiled_pack_revision == expected_revision do
      {:ok, :matched}
    else
      {:error,
       {:stale_installation_revision,
        %{
          installation_id: installation.id,
          attempted_revision: expected_revision,
          current_revision: installation.compiled_pack_revision
        }}}
    end
  end

  defp ensure_expected_installation_revision(_installation, attempted_revision) do
    {:error, {:invalid_installation_revision, attempted_revision}}
  end

  defp deserialize_compiled_pack(compiled_manifest) when is_map(compiled_manifest) do
    Serializer.deserialize_compiled(compiled_manifest)
  end

  defp resolve_trace_id(_repo, _subject_id, trace_id)
       when is_binary(trace_id) and byte_size(trace_id) > 0,
       do: {:ok, trace_id}

  defp resolve_trace_id(repo, subject_id, nil) do
    case SQL.query(repo, @load_trace_sql, [subject_id]) do
      {:ok, %{rows: [[trace_id]]}} when is_binary(trace_id) and byte_size(trace_id) > 0 ->
        {:ok, trace_id}

      {:ok, %{rows: []}} ->
        {:error, {:trace_id_not_found_for_subject, subject_id}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp resolve_supersession_context(_repo, _subject, _installation, nil), do: {:ok, nil}

  defp resolve_supersession_context(repo, subject, installation, supersession_opts) do
    execution_id = supersession_opts.supersedes_execution_id

    case SQL.query(repo, @load_superseded_execution_sql, [dump_uuid!(execution_id)]) do
      {:ok, %{rows: [[id, installation_id, subject_id, supersession_depth]]}} ->
        normalized_installation_id = load_uuid!(installation_id)
        normalized_subject_id = load_uuid!(subject_id)

        cond do
          normalized_installation_id != installation.id ->
            {:error, {:superseded_execution_installation_mismatch, execution_id}}

          normalized_subject_id != subject.id ->
            {:error, {:superseded_execution_subject_mismatch, execution_id}}

          true ->
            {:ok,
             %{
               supersedes_execution_id: load_uuid!(id),
               supersession_reason: supersession_opts.supersession_reason,
               prior_supersession_depth: supersession_depth,
               supersession_depth: supersession_depth + 1
             }}
        end

      {:ok, %{rows: []}} ->
        {:error, {:superseded_execution_not_found, execution_id}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp active_execution_present?(repo, subject_id) do
    case SQL.query(repo, @active_execution_sql, [dump_uuid!(subject_id), @active_dispatch_states]) do
      {:ok, %{num_rows: 0}} -> false
      {:ok, %{num_rows: num_rows}} when num_rows > 0 -> true
      {:error, error} -> raise error
    end
  end

  defp fetch_recipe(%CompiledPack{} = compiled_pack, recipe_ref) do
    case Map.fetch(compiled_pack.recipes_by_ref, normalize_identifier(recipe_ref)) do
      {:ok, %ExecutionRecipeSpec{} = recipe} -> {:ok, recipe}
      :error -> {:error, {:unknown_recipe_ref, recipe_ref}}
    end
  end

  defp binding_snapshot(binding_config, %ExecutionRecipeSpec{} = recipe) do
    recipe_ref = normalize_identifier(recipe.recipe_ref)

    binding =
      binding_config
      |> get_in(["execution_bindings", recipe_ref])
      |> case do
        nil -> %{}
        value -> value |> normalize_map() |> compact_map()
      end

    {:ok,
     binding
     |> Map.put_new("placement_ref", normalize_identifier(recipe.placement_ref))
     |> Map.put_new("execution_params", normalize_map(recipe.execution_params))
     |> maybe_put_optional_map(binding, "connector_capability")
     |> maybe_put_map("connector_bindings")
     |> maybe_put_map("actor_bindings")
     |> maybe_put_map("evidence_bindings")}
  end

  defp dispatch_envelope(%ExecutionRecipeSpec{} = recipe, binding_snapshot) do
    %{
      "recipe_ref" => normalize_identifier(recipe.recipe_ref),
      "runtime_class" => normalize_identifier(recipe.runtime_class),
      "placement_ref" => Map.get(binding_snapshot, "placement_ref"),
      "execution_params" => Map.get(binding_snapshot, "execution_params", %{}),
      "grant_spec" => normalize_map(recipe.grant_spec),
      "authority_decision_ref" => Map.get(binding_snapshot, "authority_decision_ref"),
      "credential_lease_ref" => Map.get(binding_snapshot, "credential_lease_ref"),
      "credentials_required" => Map.get(binding_snapshot, "credentials_required"),
      "no_credentials_posture_ref" => Map.get(binding_snapshot, "no_credentials_posture_ref"),
      "dispatch_ref_requirements" => normalize_map(recipe.dispatch_ref_requirements)
    }
    |> compact_map()
  end

  defp intent_snapshot(%ExecutionRecipeSpec{} = recipe, binding_snapshot, dispatch_envelope) do
    %{
      "recipe_ref" => normalize_identifier(recipe.recipe_ref),
      "runtime_class" => normalize_identifier(recipe.runtime_class),
      "required_lifecycle_hints" => normalize_identifier_list(recipe.required_lifecycle_hints),
      "binding_snapshot" => normalize_map(binding_snapshot),
      "dispatch_envelope" => normalize_map(dispatch_envelope)
    }
  end

  defp update_subject_lifecycle(repo, subject, to_state, now) do
    case SQL.query(repo, @update_subject_sql, [
           dump_uuid!(subject.id),
           to_state,
           now,
           subject.row_version
         ]) do
      {:ok, %{rows: [[row_version]]}} -> {:ok, row_version}
      {:ok, %{rows: []}} -> {:error, {:stale_subject_row, subject.id}}
      {:error, error} -> {:error, error}
    end
  end

  defp insert_execution_record(
         repo,
         subject,
         installation,
         execution_attrs,
         now
       ) do
    with :ok <- validate_dispatch_ref_payloads(subject, installation, execution_attrs),
         :ok <- validate_execution_payloads(execution_attrs) do
      execution_id = Ecto.UUID.generate()

      params = [
        dump_uuid!(execution_id),
        installation.tenant_id,
        installation.id,
        dump_uuid!(subject.id),
        dump_uuid(Map.get(execution_attrs, :barrier_id)),
        execution_attrs.recipe_ref,
        installation.compiled_pack_revision,
        execution_attrs.binding_snapshot,
        execution_attrs.dispatch_envelope,
        execution_attrs.intent_snapshot,
        execution_attrs.submission_dedupe_key,
        execution_attrs.trace_id,
        execution_attrs.causation_id,
        now,
        %{},
        %{},
        %{},
        dump_uuid(execution_attrs.supersedes_execution_id),
        execution_attrs.supersession_reason,
        execution_attrs.supersession_depth || 0
      ]

      case SQL.query(repo, @insert_execution_sql, params) do
        {:ok, %{rows: [[inserted_execution_id]]}} -> {:ok, load_uuid!(inserted_execution_id)}
        {:error, error} -> {:error, error}
      end
    end
  end

  defp validate_dispatch_ref_payloads(subject, installation, execution_attrs) do
    DispatchEnvelopeRefValidator.validate(%{
      tenant_id: installation.tenant_id,
      installation_id: installation.id,
      subject_id: subject.id,
      compiled_pack_revision: installation.compiled_pack_revision,
      binding_snapshot: execution_attrs.binding_snapshot,
      dispatch_envelope: execution_attrs.dispatch_envelope,
      intent_snapshot: execution_attrs.intent_snapshot,
      submission_dedupe_key: execution_attrs.submission_dedupe_key,
      trace_id: execution_attrs.trace_id
    })
  end

  defp validate_execution_payloads(execution_attrs) do
    Enum.reduce_while(
      [:binding_snapshot, :dispatch_envelope, :intent_snapshot],
      :ok,
      fn field, :ok ->
        case PayloadBoundary.validate_execution_column(field, Map.fetch!(execution_attrs, field)) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end
    )
  end

  defp workflow_handoff(repo, dispatch_context, changes, now) do
    with {:ok, outbox_row} <- workflow_start_outbox_row(dispatch_context, changes, now),
         :ok <- insert_workflow_start_outbox(repo, outbox_row, now) do
      {:ok,
       %{
         provider: :temporal_workflow,
         workflow_type: :execution_attempt,
         workflow_module: "Mezzanine.Workflows.ExecutionAttempt",
         workflow_runtime_boundary: "Mezzanine.WorkflowRuntime",
         execution_id: changes.execution_id,
         scheduled_at: now,
         release_manifest_ref: outbox_row.release_manifest_ref,
         outbox_id: outbox_row.outbox_id,
         workflow_id: outbox_row.workflow_id,
         idempotency_key: outbox_row.idempotency_key,
         command_receipt_ref: outbox_row.command_receipt_ref,
         workflow_input_ref: outbox_row.workflow_input_ref,
         authority_packet_ref: outbox_row.authority_packet_ref,
         permission_decision_ref: outbox_row.permission_decision_ref,
         dispatch_job_args: workflow_start_dispatch_args(outbox_row)
       }}
    end
  end

  defp workflow_start_outbox_row(dispatch_context, changes, now) do
    subject = dispatch_context.subject
    installation = dispatch_context.installation
    execution_id = changes.execution_id
    submission_dedupe_key = changes.submission_dedupe_key
    release_manifest_ref = @workflow_start_release_manifest_ref
    workflow_type = "execution_attempt"
    command_id = "execution:#{execution_id}"
    resource_ref = "execution://#{execution_id}"

    workflow_id =
      deterministic_workflow_id(%{
        tenant_ref: installation.tenant_id,
        resource_ref: resource_ref,
        workflow_type: workflow_type,
        command_id: command_id,
        release_manifest_ref: release_manifest_ref
      })

    attrs = %{
      outbox_id: "workflow-start://#{execution_id}",
      tenant_ref: installation.tenant_id,
      installation_ref: "installation://#{installation.id}",
      workspace_ref: workspace_ref(changes.binding_snapshot),
      project_ref: subject_project_ref(subject),
      environment_ref: "environment://default",
      principal_ref: principal_ref(dispatch_context.actor_ref),
      system_actor_ref: "system://mezzanine/lifecycle_evaluator",
      resource_ref: resource_ref,
      command_envelope_ref: "execution-command://#{execution_id}",
      command_receipt_ref: "execution-record://#{execution_id}/queued",
      command_id: command_id,
      workflow_type: workflow_type,
      workflow_id: workflow_id,
      workflow_version: "execution_attempt.v1",
      workflow_input_version: "Mezzanine.WorkflowExecutionLifecycleInput.v1",
      workflow_input_ref: "workflow-input://#{execution_id}",
      authority_packet_ref: "citadel-authority-request://#{execution_id}",
      permission_decision_ref: "citadel-permission-decision://#{execution_id}",
      canonical_idempotency_key: submission_dedupe_key,
      causation_id: dispatch_context.causation_id,
      idempotency_key: submission_dedupe_key,
      dedupe_scope: "installation:#{installation.id}:execution:#{execution_id}",
      trace_id: dispatch_context.trace_id,
      correlation_id: dispatch_context.causation_id,
      release_manifest_ref: release_manifest_ref,
      payload_hash:
        payload_hash(%{
          execution_id: execution_id,
          subject_id: subject.id,
          recipe_ref: dispatch_context.recipe_ref,
          submission_dedupe_key: submission_dedupe_key,
          workflow_id: workflow_id
        }),
      payload_ref: "workflow-input://#{execution_id}",
      dispatch_state: "queued",
      retry_count: 0,
      last_error_class: "none",
      available_at: DateTime.to_iso8601(now)
    }

    new_workflow_start_outbox_row(attrs)
  end

  defp insert_workflow_start_outbox(repo, outbox_row, now) do
    params = [
      outbox_row.outbox_id,
      outbox_row.tenant_ref,
      outbox_row.installation_ref,
      outbox_row.workspace_ref,
      outbox_row.project_ref,
      outbox_row.environment_ref,
      outbox_row.principal_ref,
      outbox_row.system_actor_ref,
      outbox_row.resource_ref,
      outbox_row.command_envelope_ref,
      outbox_row.command_receipt_ref,
      outbox_row.command_id,
      outbox_row.workflow_type,
      outbox_row.workflow_id,
      outbox_row.workflow_version,
      outbox_row.workflow_input_version,
      outbox_row.workflow_input_ref,
      outbox_row.authority_packet_ref,
      outbox_row.permission_decision_ref,
      outbox_row.idempotency_key,
      outbox_row.dedupe_scope,
      outbox_row.trace_id,
      outbox_row.correlation_id,
      outbox_row.release_manifest_ref,
      outbox_row.payload_hash,
      outbox_row.payload_ref,
      outbox_row.dispatch_state,
      outbox_row.retry_count,
      outbox_row.last_error_class,
      outbox_row.available_at,
      now
    ]

    case SQL.query(repo, @insert_workflow_start_outbox_sql, params) do
      {:ok, %{rows: [[_outbox_id]]}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp workflow_start_job_changeset(%{workflow_handoff: workflow_handoff}) do
    Oban.Job.new(workflow_handoff.dispatch_job_args,
      queue: @workflow_start_outbox_queue,
      worker: @workflow_start_outbox_worker,
      max_attempts: 20,
      unique: @workflow_start_unique
    )
  end

  defp new_workflow_start_outbox_row(attrs) do
    attrs
    |> Map.put_new(:dispatch_state, "queued")
    |> Map.put_new(:retry_count, 0)
    |> Map.put_new(:release_manifest_ref, @workflow_start_release_manifest_ref)
    |> WorkflowStartOutboxPayload.new()
  end

  defp deterministic_workflow_id(attrs) do
    [
      "tenant",
      Map.fetch!(attrs, :tenant_ref),
      "resource",
      Map.fetch!(attrs, :resource_ref),
      "workflow",
      Map.fetch!(attrs, :workflow_type),
      "command",
      Map.fetch!(attrs, :command_id),
      "release",
      Map.fetch!(attrs, :release_manifest_ref)
    ]
    |> Enum.join(":")
  end

  defp workflow_start_dispatch_args(outbox_row) do
    outbox_row
    |> Map.from_struct()
    |> Map.take(
      @workflow_start_required_fields ++
        @workflow_start_correlation_fields ++ [:payload_ref, :retry_count, :oban_job_ref]
    )
    |> stringify_keys()
  end

  defp workspace_ref(binding_snapshot) do
    case Map.get(binding_snapshot, "placement_ref") do
      value when is_binary(value) and value != "" -> "workspace-placement://#{value}"
      _other -> nil
    end
  end

  defp subject_project_ref(%{installation_id: installation_id}) do
    "installation://#{installation_id}"
  end

  defp principal_ref(actor_ref) when is_map(actor_ref) do
    case Map.get(actor_ref, :kind) || Map.get(actor_ref, "kind") do
      value when is_atom(value) -> "actor://#{value}"
      value when is_binary(value) and value != "" -> "actor://#{value}"
      _other -> "actor://lifecycle_evaluator"
    end
  end

  defp payload_hash(material) do
    encoded = :erlang.term_to_binary(material)
    "sha256:" <> Base.encode16(:crypto.hash(:sha256, encoded), case: :lower)
  end

  defp stringify_keys(map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), value} end)
    |> Map.new()
  end

  defp append_audit_fact(repo, audit_attrs, occurred_at) do
    audit_attrs
    |> Map.put(:occurred_at, occurred_at)
    |> AuditAppend.append_fact(repo: repo)
    |> case do
      {:ok, _fact} -> {:ok, :recorded}
      {:error, error} -> {:error, error}
    end
  end

  defp upsert_execution_lineage(
         repo,
         trace_id,
         causation_id,
         tenant_id,
         installation_id,
         subject_id,
         execution_id,
         now
       ) do
    case SQL.query(repo, @upsert_lineage_sql, [
           trace_id,
           causation_id,
           tenant_id,
           installation_id,
           subject_id,
           execution_id,
           [],
           now
         ]) do
      {:ok, _result} -> {:ok, :upserted}
      {:error, error} -> {:error, error}
    end
  end

  defp cycle_bound_reached?(compiled_pack, supersession_context) do
    case supersession_context do
      %{supersession_depth: depth} -> depth > max_supersession_depth(compiled_pack)
      _other -> false
    end
  end

  defp max_supersession_depth(%CompiledPack{} = compiled_pack) do
    compiled_pack.manifest.max_supersession_depth || @default_max_supersession_depth
  end

  defp lifecycle_advanced_payload(from_state, to_state, recipe_ref, supersession_context) do
    %{
      "from" => from_state,
      "to" => to_state,
      "trigger" => serialize_trigger({:execution_requested, recipe_ref})
    }
    |> maybe_put_supersession_payload(supersession_context)
  end

  defp execution_dispatched_payload(
         recipe_ref,
         submission_dedupe_key,
         workflow_handoff,
         supersession_context
       ) do
    %{
      "recipe_ref" => recipe_ref,
      "dispatch_state" => "queued",
      "submission_dedupe_key" => submission_dedupe_key,
      "workflow_runtime_boundary" => workflow_handoff.workflow_runtime_boundary,
      "temporal_workflow" => workflow_handoff.workflow_module,
      "release_manifest_ref" => workflow_handoff.release_manifest_ref
    }
    |> maybe_put_supersession_payload(supersession_context)
  end

  defp maybe_put_supersession_payload(payload, nil), do: payload

  defp maybe_put_supersession_payload(payload, supersession_context) do
    payload
    |> Map.put("supersedes_execution_id", supersession_id(supersession_context))
    |> Map.put("supersession_reason", supersession_reason_value(supersession_context))
    |> Map.put("supersession_depth", supersession_depth_value(supersession_context))
  end

  defp supersession_id(%{supersedes_execution_id: execution_id}), do: execution_id
  defp supersession_id(_other), do: nil

  defp supersession_reason_value(%{supersession_reason: reason}) when is_atom(reason),
    do: Atom.to_string(reason)

  defp supersession_reason_value(%{supersession_reason: reason}) when is_binary(reason),
    do: reason

  defp supersession_reason_value(_other), do: nil

  defp supersession_depth_value(%{supersession_depth: depth}), do: depth
  defp supersession_depth_value(_other), do: 0

  defp subject_snapshot(subject) do
    SubjectSnapshot.new(%{
      subject_kind: subject.subject_kind,
      lifecycle_state: subject.lifecycle_state,
      payload: subject.payload
    })
  end

  defp maybe_put_map(map, key) do
    Map.put_new_lazy(map, key, fn -> %{} end)
  end

  defp maybe_put_optional_map(map, source, key) do
    case Map.get(source, key) do
      value when is_map(value) -> Map.put(map, key, normalize_map(value))
      _other -> map
    end
  end

  defp execute(repo, sql, params) do
    case SQL.query(repo, sql, params) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp default_causation_id(subject_id, now) do
    "lifecycle-evaluator:#{subject_id}:#{DateTime.to_unix(now, :microsecond)}"
  end

  defp normalize_supersession_opts(opts) do
    supersedes_execution_id = Keyword.get(opts, :supersedes_execution_id)
    supersession_reason = Keyword.get(opts, :supersession_reason)

    case {supersedes_execution_id, supersession_reason} do
      {nil, nil} ->
        {:ok, nil}

      {nil, _reason} ->
        {:error, :missing_supersedes_execution_id}

      {_execution_id, nil} ->
        {:error, :missing_supersession_reason}

      {execution_id, reason} ->
        with {:ok, normalized_execution_id} <- normalize_uuid(execution_id),
             {:ok, normalized_reason} <- normalize_supersession_reason(reason) do
          {:ok,
           %{
             supersedes_execution_id: normalized_execution_id,
             supersession_reason: normalized_reason
           }}
        end
    end
  end

  defp normalize_uuid(value) when is_binary(value) do
    case Ecto.UUID.cast(value) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> {:error, {:invalid_uuid, value}}
    end
  end

  defp normalize_uuid(value), do: {:error, {:invalid_uuid, value}}

  defp normalize_supersession_reason(reason) when reason in @supersession_reasons,
    do: {:ok, reason}

  defp normalize_supersession_reason(reason) when is_binary(reason) do
    case Map.fetch(@supersession_reason_lookup, reason) do
      {:ok, normalized_reason} -> {:ok, normalized_reason}
      :error -> {:error, {:invalid_supersession_reason, reason}}
    end
  end

  defp normalize_supersession_reason(reason),
    do: {:error, {:invalid_supersession_reason, reason}}

  defp serialize_trigger({:execution_requested, recipe_ref}) do
    %{
      "kind" => "execution_requested",
      "recipe_ref" => normalize_identifier(recipe_ref)
    }
  end

  defp serialize_trigger({:execution_completed, recipe_ref}) do
    %{
      "kind" => "execution_completed",
      "recipe_ref" => normalize_identifier(recipe_ref)
    }
  end

  defp serialize_trigger({:execution_failed, recipe_ref}) do
    %{
      "kind" => "execution_failed",
      "recipe_ref" => normalize_identifier(recipe_ref)
    }
  end

  defp serialize_trigger({:execution_failed, recipe_ref, failure_kind}) do
    %{
      "kind" => "execution_failed",
      "recipe_ref" => normalize_identifier(recipe_ref),
      "failure_kind" => normalize_identifier(failure_kind)
    }
  end

  defp serialize_trigger({:join_completed, join_step_ref}) do
    %{
      "kind" => "join_completed",
      "join_step_ref" => normalize_identifier(join_step_ref)
    }
  end

  defp normalize_identifier(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_identifier(value) when is_binary(value), do: value

  defp normalize_identifier(value) do
    raise ArgumentError,
          "expected identifier to be an atom or string, got: #{inspect(value)}"
  end

  defp normalize_identifier_list(values) when is_list(values) do
    values
    |> Enum.map(&normalize_identifier/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_identifier_list(_other), do: []

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_other), do: %{}

  defp compact_map(value) when is_map(value) do
    value
    |> Enum.reduce(%{}, fn {key, nested}, acc ->
      case compact_value(nested) do
        nil -> acc
        compacted -> Map.put(acc, key, compacted)
      end
    end)
  end

  defp compact_value(value) when is_map(value), do: compact_map(value)
  defp compact_value(value) when is_list(value), do: Enum.map(value, &compact_value/1)
  defp compact_value(value), do: value

  defp normalize_value(nil), do: nil
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp dump_uuid(nil), do: nil
  defp dump_uuid(value), do: Ecto.UUID.dump!(value)
  defp dump_uuid!(value), do: Ecto.UUID.dump!(value)

  defp load_uuid!(value) when is_binary(value) and byte_size(value) == 16,
    do: Ecto.UUID.load!(value)

  defp load_uuid!(value) when is_binary(value), do: value
end
