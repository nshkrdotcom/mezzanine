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
  alias Mezzanine.Execution.DispatchState
  alias Mezzanine.Execution.Repo
  alias Mezzanine.Lifecycle.Evaluator, as: PackEvaluator
  alias Mezzanine.Lifecycle.SubjectSnapshot
  alias Mezzanine.Pack.{CompiledPack, ExecutionRecipeSpec, Serializer}

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
  @supersession_reasons [
    :retry_transient,
    :retry_semantic,
    :operator_replan,
    :pack_revision_change,
    :manual_retry
  ]
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

  @upsert_lineage_sql """
  INSERT INTO execution_lineage_records (
    id,
    trace_id,
    causation_id,
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
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    $6,
    $7,
    $7
  )
  ON CONFLICT (execution_id) DO UPDATE
  SET trace_id = EXCLUDED.trace_id,
      causation_id = EXCLUDED.causation_id,
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
      |> Multi.run(:workflow_handoff, fn _repo, %{execution_id: execution_id} ->
        workflow_handoff(execution_id, now)
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
      "grant_spec" => normalize_map(recipe.grant_spec)
    }
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

  defp workflow_handoff(execution_id, now) do
    {:ok,
     %{
       provider: :temporal_workflow,
       workflow_type: :execution_attempt,
       workflow_module: "Mezzanine.Workflows.ExecutionAttempt",
       workflow_runtime_boundary: "Mezzanine.WorkflowRuntime",
       execution_id: execution_id,
       scheduled_at: now,
       release_manifest_ref: "phase4-v6-milestone31-temporal-cutover"
     }}
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
         installation_id,
         subject_id,
         execution_id,
         now
       ) do
    case SQL.query(repo, @upsert_lineage_sql, [
           trace_id,
           causation_id,
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
    reason
    |> String.to_existing_atom()
    |> normalize_supersession_reason()
  rescue
    ArgumentError -> {:error, {:invalid_supersession_reason, reason}}
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
