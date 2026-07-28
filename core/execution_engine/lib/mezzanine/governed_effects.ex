defmodule Mezzanine.GovernedEffects do
  @moduledoc """
  Durable orchestration boundary for one reviewed governed effect.

  Effect identity, exact authority refs, the reviewed operation, and the pinned
  tool manifest are immutable snapshots on the existing execution ledger.
  Provider credentials, process handles, file contents, and workspace paths are
  never accepted by this boundary.
  """

  require Ash.Query

  alias Mezzanine.AgentRuntime.Support
  alias Mezzanine.Effects.EffectRecord
  alias Mezzanine.Execution.{ExecutionRecord, LifecycleContinuation, Repo}
  alias Mezzanine.Reviews

  @capability_id "codex.session.turn"
  @effect_mode "managed_account_local_effect"
  @recipe_ref "codex_reviewed_local_effect"
  @terminal_states [:completed, :failed, :cancelled, :rejected, :stalled]
  @ambiguity_states ~w(dispatch_unknown outcome_unknown receipt_missing)
  @success_states ~w(completed success succeeded)
  @failure_states ~w(failed failure)
  @cancel_states ~w(cancelled canceled)
  @forbidden_keys ~w(
    api_key authorization base_url callback codex_home env environment
    home password provider_key rollback_callback secret secrets token workspace_root
  )
  @required_open_fields ~w(
    tenant_id installation_id subject_id run_id review_unit_id effect_ref run_ref
    turn_ref command_ref decision_ref grant_ref review_ref idempotency_key target_ref
    trace_id causation_id attempt_ref pinned_tool_manifest reviewed_operation actor_ref
  )a

  @type operation_result :: %{
          required(:status) => :created | :reused | :updated,
          required(:execution) => ExecutionRecord.t(),
          required(:effect_record) => EffectRecord.t(),
          optional(:continuation) => LifecycleContinuation.t()
        }

  @doc """
  Persists one exact effect command against a durable subject, run, and review.

  The review may still be pending. `begin_dispatch/2` performs a fresh durable
  read and refuses dispatch until the linked review is accepted.
  """
  @spec open(map() | keyword()) :: {:ok, operation_result()} | {:error, term()}
  def open(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- validate_open_attrs(attrs),
         {:ok, %{review_unit: review_unit}} <- review_detail(attrs),
         :ok <- ensure_review_link(review_unit, attrs),
         execution_attrs = execution_attrs(attrs, review_unit),
         {:ok, status, execution} <- ensure_execution(execution_attrs),
         {:ok, effect_record} <- to_effect_record(execution) do
      {:ok, %{status: status, execution: execution, effect_record: effect_record}}
    end
  end

  def open(_attrs), do: {:error, :invalid_governed_effect_command}

  @doc "Starts dispatch only after an accepted persisted review and version check."
  @spec begin_dispatch(ExecutionRecord.t() | Ecto.UUID.t(), map() | keyword()) ::
          {:ok, operation_result()} | {:error, term()}
  def begin_dispatch(execution_or_id, attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         {:ok, execution} <- load_execution(execution_or_id),
         :ok <- expected_version(execution, attrs),
         :ok <- ensure_state(execution, [:queued]),
         {:ok, review_detail} <- review_detail_from_execution(execution),
         :ok <- ensure_accepted_review(review_detail, execution),
         {:ok, execution} <-
           ExecutionRecord.mark_dispatching(execution, %{
             trace_id: required!(attrs, :trace_id),
             causation_id: required!(attrs, :causation_id)
           }),
         {:ok, effect_record} <- to_effect_record(execution) do
      {:ok, %{status: :updated, execution: execution, effect_record: effect_record}}
    end
  end

  def begin_dispatch(_execution_or_id, _attrs), do: {:error, :invalid_dispatch_command}

  @doc "Commits the lower accepted identity without storing a process handle."
  @spec record_accepted(ExecutionRecord.t() | Ecto.UUID.t(), map() | keyword()) ::
          {:ok, operation_result()} | {:error, term()}
  def record_accepted(execution_or_id, attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- validate_runtime_attrs(attrs),
         {:ok, execution} <- load_execution(execution_or_id),
         :ok <- expected_version(execution, attrs),
         :ok <- ensure_state(execution, [:in_flight]),
         :ok <- ensure_attempt_identity(execution, attrs),
         lower_receipt <- accepted_receipt(execution, attrs),
         {:ok, execution} <-
           ExecutionRecord.record_accepted(execution, %{
             submission_ref: required_map!(attrs, :submission_ref),
             lower_receipt: lower_receipt,
             trace_id: required!(attrs, :trace_id),
             causation_id: required!(attrs, :causation_id),
             actor_ref: required_map!(attrs, :actor_ref)
           }),
         {:ok, effect_record} <- to_effect_record(execution) do
      {:ok, %{status: :updated, execution: execution, effect_record: effect_record}}
    end
  end

  def record_accepted(_execution_or_id, _attrs), do: {:error, :invalid_acceptance_command}

  @doc """
  Commits one terminal or ambiguous receipt exactly once and enqueues a named
  continuation. Ambiguity accepts only `reconcile_effect_outcome`; the effect
  execution itself is never retried.
  """
  @spec record_receipt(ExecutionRecord.t() | Ecto.UUID.t(), map() | keyword()) ::
          {:ok, operation_result()} | {:error, term()}
  def record_receipt(execution_or_id, attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         :ok <- validate_runtime_attrs(attrs),
         {:ok, execution} <- load_execution(execution_or_id),
         :ok <- expected_version_or_terminal_replay(execution, attrs),
         {:ok, receipt_kind, receipt_state} <- receipt_kind(attrs),
         {:ok, target} <- continuation_target(attrs, receipt_kind),
         {:ok, result} <-
           persist_receipt_once(execution, attrs, receipt_kind, receipt_state, target) do
      {:ok, result}
    end
  end

  def record_receipt(_execution_or_id, _attrs), do: {:error, :invalid_receipt_command}

  @doc "Reads one effect through its durable execution identity."
  @spec fetch(Ecto.UUID.t()) :: {:ok, operation_result()} | {:error, term()}
  def fetch(execution_id) when is_binary(execution_id) do
    with {:ok, execution} <- load_execution(execution_id),
         {:ok, effect_record} <- to_effect_record(execution) do
      {:ok, %{status: :reused, execution: execution, effect_record: effect_record}}
    end
  end

  @doc "Reads one effect through its installation-scoped idempotency identity."
  @spec fetch_by_idempotency(String.t(), String.t()) ::
          {:ok, operation_result()} | {:error, term()}
  def fetch_by_idempotency(installation_id, idempotency_key)
      when is_binary(installation_id) and is_binary(idempotency_key) do
    case existing_execution(installation_id, idempotency_key) do
      {:ok, %ExecutionRecord{id: id}} -> fetch(id)
      {:ok, nil} -> {:error, :effect_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_open_attrs(attrs) do
    with :ok <- require_fields(attrs, @required_open_fields),
         :ok <- Support.reject_unsafe(attrs, :unsafe_governed_effect_command),
         :ok <- reject_forbidden_keys(attrs),
         :ok <- valid_uuid_field(attrs, :subject_id),
         :ok <- valid_uuid_field(attrs, :run_id),
         :ok <- valid_uuid_field(attrs, :review_unit_id),
         :ok <- validate_tool_manifest(required_map!(attrs, :pinned_tool_manifest)),
         :ok <- validate_reviewed_operation(required_map!(attrs, :reviewed_operation)),
         true <- is_map(value(attrs, :actor_ref)) do
      :ok
    else
      false -> {:error, :invalid_governed_effect_command}
      {:error, _reason} = error -> error
    end
  rescue
    ArgumentError -> {:error, :invalid_governed_effect_command}
  end

  defp validate_runtime_attrs(attrs) do
    with :ok <- Support.reject_unsafe(attrs, :unsafe_governed_effect_runtime_payload),
         :ok <- reject_forbidden_keys(attrs) do
      :ok
    end
  end

  defp validate_tool_manifest(manifest) do
    with :ok <- require_fields(manifest, [:manifest_ref, :manifest_hash, :action_ids]),
         true <- sha256?(value(manifest, :manifest_hash)),
         action_ids when is_list(action_ids) <- value(manifest, :action_ids),
         true <- "create_or_replace_one_named_text_file" in action_ids do
      :ok
    else
      _other -> {:error, :invalid_pinned_tool_manifest}
    end
  end

  defp validate_reviewed_operation(operation) do
    with :ok <-
           require_fields(operation, [
             :operation,
             :workspace_ref,
             :file_ref,
             :relative_path,
             :content_digest
           ]),
         true <- value(operation, :operation) == "create_or_replace",
         true <- relative_file?(value(operation, :relative_path)),
         true <- sha256?(value(operation, :content_digest)),
         true <- Support.safe_ref?(value(operation, :workspace_ref)),
         true <- Support.safe_ref?(value(operation, :file_ref)) do
      :ok
    else
      _other -> {:error, :invalid_reviewed_operation}
    end
  end

  defp execution_attrs(attrs, review_unit) do
    operation = stringify(required_map!(attrs, :reviewed_operation))

    %{
      tenant_id: required!(attrs, :tenant_id),
      installation_id: required!(attrs, :installation_id),
      subject_id: required!(attrs, :subject_id),
      recipe_ref: @recipe_ref,
      compiled_pack_revision: positive_integer(value(attrs, :compiled_pack_revision), 1),
      binding_snapshot: %{
        "effect_mode" => @effect_mode,
        "target_ref" => required!(attrs, :target_ref),
        "workspace_ref" => value(operation, :workspace_ref),
        "file_ref" => value(operation, :file_ref)
      },
      dispatch_envelope: %{
        "capability_id" => @capability_id,
        "effect_mode" => @effect_mode,
        "target_ref" => required!(attrs, :target_ref),
        "attempt_ref" => required!(attrs, :attempt_ref),
        "pinned_tool_manifest" => stringify(required_map!(attrs, :pinned_tool_manifest)),
        "reviewed_operation" => operation
      },
      intent_snapshot: %{
        "contract_version" => 1,
        "effect_ref" => required!(attrs, :effect_ref),
        "run_ref" => required!(attrs, :run_ref),
        "run_id" => required!(attrs, :run_id),
        "turn_ref" => required!(attrs, :turn_ref),
        "command_ref" => required!(attrs, :command_ref),
        "decision_ref" => required!(attrs, :decision_ref),
        "grant_ref" => required!(attrs, :grant_ref),
        "review_ref" => required!(attrs, :review_ref),
        "review_unit_id" => required!(attrs, :review_unit_id),
        "review_status_at_command" => Atom.to_string(review_unit.status),
        "target_ref" => required!(attrs, :target_ref),
        "idempotency_key" => required!(attrs, :idempotency_key)
      },
      submission_dedupe_key: required!(attrs, :idempotency_key),
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      actor_ref: required_map!(attrs, :actor_ref)
    }
  end

  defp ensure_execution(attrs) do
    with {:ok, existing} <-
           existing_execution(attrs.installation_id, attrs.submission_dedupe_key) do
      case existing do
        nil ->
          case ExecutionRecord.dispatch(attrs) do
            {:ok, execution} -> {:ok, :created, execution}
            {:error, reason} -> {:error, reason}
          end

        %ExecutionRecord{} = execution ->
          if same_effect_command?(execution, attrs),
            do: {:ok, :reused, execution},
            else: {:error, :effect_idempotency_conflict}
      end
    end
  end

  defp same_effect_command?(execution, attrs) do
    execution.tenant_id == attrs.tenant_id and
      execution.subject_id == attrs.subject_id and
      execution.intent_snapshot == attrs.intent_snapshot and
      execution.binding_snapshot == attrs.binding_snapshot and
      execution.dispatch_envelope == attrs.dispatch_envelope
  end

  defp review_detail(attrs) do
    case Reviews.review_detail(required!(attrs, :tenant_id), required!(attrs, :review_unit_id)) do
      {:ok, %{review_unit: _review_unit} = detail} -> {:ok, detail}
      {:error, reason} -> {:error, {:review_unavailable, reason}}
    end
  end

  defp review_detail_from_execution(execution) do
    review_detail(%{
      tenant_id: execution.tenant_id,
      review_unit_id: map_value(execution.intent_snapshot, :review_unit_id)
    })
  end

  defp ensure_review_link(review_unit, attrs) do
    cond do
      review_unit.work_object_id != required!(attrs, :subject_id) ->
        {:error, :review_subject_mismatch}

      review_unit.run_id != required!(attrs, :run_id) ->
        {:error, :review_run_mismatch}

      review_unit.status in [:rejected, :waived, :escalated] ->
        {:error, {:review_not_dispatchable, review_unit.status}}

      true ->
        :ok
    end
  end

  defp ensure_accepted_review(%{review_unit: review_unit, decisions: decisions}, execution) do
    intent = execution.intent_snapshot

    cond do
      review_unit.work_object_id != execution.subject_id ->
        {:error, :review_subject_mismatch}

      review_unit.run_id != map_value(intent, :run_id) ->
        {:error, :review_run_mismatch}

      review_unit.status != :accepted ->
        {:error, {:review_not_accepted, review_unit.status}}

      not Enum.any?(decisions, &(Map.get(&1, :decision) == :accept)) ->
        {:error, :accepted_review_decision_missing}

      not present?(map_value(intent, :decision_ref)) or
          not present?(map_value(intent, :grant_ref)) ->
        {:error, :authority_identity_missing}

      true ->
        :ok
    end
  end

  defp accepted_receipt(execution, attrs) do
    attrs
    |> value(:lower_receipt)
    |> Kernel.||(%{})
    |> stringify()
    |> Map.merge(%{
      "receipt_state" => "accepted",
      "attempt_ref" => effect_attempt_ref(execution),
      "execution_ref" => execution_ref(execution)
    })
  end

  defp ensure_attempt_identity(execution, attrs) do
    attempted = attrs |> required_map!(:submission_ref) |> value(:attempt_ref)

    if attempted == effect_attempt_ref(execution),
      do: :ok,
      else: {:error, :attempt_identity_mismatch}
  end

  defp receipt_kind(attrs) do
    state = attrs |> required!(:receipt_state) |> to_string()

    cond do
      state in @success_states -> {:ok, :completed, state}
      state in @failure_states -> {:ok, :failed, state}
      state in @cancel_states -> {:ok, :cancelled, state}
      state == "ambiguous" -> ambiguity_kind(attrs)
      state in @ambiguity_states -> {:ok, :ambiguous, state}
      true -> {:error, {:unsupported_effect_receipt_state, state}}
    end
  end

  defp ambiguity_kind(attrs) do
    state = attrs |> required!(:ambiguity_state) |> to_string()

    if state in @ambiguity_states,
      do: {:ok, :ambiguous, state},
      else: {:error, {:invalid_ambiguity_state, state}}
  end

  defp continuation_target(attrs, receipt_kind) do
    target = attrs |> required_map!(:continuation_target) |> stringify()

    with :ok <- validate_continuation_target(target),
         :ok <- validate_ambiguity_target(target, receipt_kind) do
      {:ok, target}
    end
  rescue
    ArgumentError -> {:error, :missing_continuation_target}
  end

  defp validate_continuation_target(%{
         "kind" => "owner_command",
         "owner" => owner,
         "command" => command,
         "idempotency_key" => key
       }) do
    if Enum.all?([owner, command, key], &present?/1),
      do: :ok,
      else: {:error, :invalid_continuation_target}
  end

  defp validate_continuation_target(%{
         "kind" => "workflow_signal",
         "workflow_id" => workflow_id,
         "signal" => signal,
         "idempotency_key" => key
       }) do
    if Enum.all?([workflow_id, signal, key], &present?/1),
      do: :ok,
      else: {:error, :invalid_continuation_target}
  end

  defp validate_continuation_target(_target), do: {:error, :invalid_continuation_target}

  defp validate_ambiguity_target(
         %{"kind" => "owner_command", "command" => "reconcile_effect_outcome"},
         :ambiguous
       ),
       do: :ok

  defp validate_ambiguity_target(_target, :ambiguous),
    do: {:error, :ambiguous_effect_requires_reconciliation_command}

  defp validate_ambiguity_target(_target, _receipt_kind), do: :ok

  defp persist_receipt_once(execution, attrs, receipt_kind, receipt_state, target) do
    receipt_ref = required!(attrs, :receipt_ref)

    cond do
      execution.dispatch_state in @terminal_states ->
        replay_terminal(execution, receipt_ref)

      execution.dispatch_state not in [:in_flight, :accepted_active] ->
        {:error, {:effect_not_receiptable, execution.dispatch_state}}

      true ->
        continuation_id = value(attrs, :continuation_id) || Ecto.UUID.generate()

        lower_receipt =
          lower_receipt(execution, attrs, receipt_kind, receipt_state, continuation_id)

        Repo.transaction(fn ->
          with {:ok, updated_execution, notifications} <-
                 persist_execution_receipt(
                   execution,
                   attrs,
                   receipt_kind,
                   receipt_state,
                   lower_receipt
                 ),
               {:ok, continuation} <-
                 enqueue_continuation(
                   updated_execution,
                   attrs,
                   receipt_kind,
                   receipt_state,
                   continuation_id,
                   target
                 ),
               {:ok, effect_record} <- to_effect_record(updated_execution) do
            %{
              status: :updated,
              execution: updated_execution,
              effect_record: effect_record,
              continuation: continuation,
              notifications: notifications
            }
          else
            {:error, reason} -> Repo.rollback(reason)
          end
        end)
        |> notify_receipt_result()
    end
  end

  defp persist_execution_receipt(execution, attrs, :completed, _state, lower_receipt) do
    result_ref = required!(attrs, :result_artifact_ref)

    update_execution(
      execution,
      :record_completed,
      terminal_attrs(attrs, lower_receipt, artifact_refs: artifact_refs(attrs, result_ref))
    )
  end

  defp persist_execution_receipt(execution, attrs, :failed, _state, lower_receipt) do
    update_execution(
      execution,
      :record_failed_outcome,
      terminal_attrs(attrs, lower_receipt,
        failure_kind: normalize_failure_kind(value(attrs, :failure_kind)),
        artifact_refs: artifact_refs(attrs)
      )
    )
  end

  defp persist_execution_receipt(execution, attrs, :cancelled, _state, lower_receipt) do
    update_execution(
      execution,
      :record_cancelled_outcome,
      terminal_attrs(attrs, lower_receipt, artifact_refs: artifact_refs(attrs))
    )
  end

  defp persist_execution_receipt(execution, attrs, :ambiguous, ambiguity_state, lower_receipt) do
    update_execution(
      execution,
      :record_ambiguous_outcome,
      terminal_attrs(attrs, lower_receipt,
        ambiguity_state: ambiguity_state,
        artifact_refs: artifact_refs(attrs)
      )
    )
  end

  defp update_execution(execution, action, attrs) do
    execution
    |> Ash.Changeset.for_update(action, attrs)
    |> Ash.update(
      authorize?: false,
      domain: Mezzanine.Execution,
      return_notifications?: true
    )
  end

  defp notify_receipt_result({:ok, %{notifications: notifications} = result}) do
    Ash.Notifier.notify(notifications)
    {:ok, Map.delete(result, :notifications)}
  end

  defp notify_receipt_result(result), do: result

  defp terminal_attrs(attrs, lower_receipt, extras) do
    %{
      receipt_id: required!(attrs, :receipt_ref),
      lower_receipt: lower_receipt,
      normalized_outcome: stringify(value(attrs, :normalized_outcome) || %{}),
      trace_id: required!(attrs, :trace_id),
      causation_id: required!(attrs, :causation_id),
      actor_ref: required_map!(attrs, :actor_ref)
    }
    |> Map.merge(Map.new(extras))
  end

  defp lower_receipt(execution, attrs, receipt_kind, receipt_state, continuation_id) do
    attrs
    |> value(:lower_receipt)
    |> Kernel.||(%{})
    |> stringify()
    |> Map.merge(%{
      "receipt_ref" => required!(attrs, :receipt_ref),
      "receipt_state" => receipt_state,
      "effect_ref" => map_value(execution.intent_snapshot, :effect_ref),
      "attempt_ref" => effect_attempt_ref(execution),
      "execution_ref" => execution_ref(execution),
      "continuation_ref" => "continuation://#{continuation_id}"
    })
    |> maybe_put(
      "ambiguity_state",
      if(receipt_kind == :ambiguous, do: receipt_state, else: nil)
    )
    |> maybe_put(
      "result_artifact_ref",
      if(receipt_kind == :completed, do: required!(attrs, :result_artifact_ref), else: nil)
    )
    |> maybe_put(
      "retry_posture",
      if(receipt_kind == :ambiguous,
        do: "reconciliation_only_effect_retry_prohibited",
        else: nil
      )
    )
  end

  defp enqueue_continuation(
         execution,
         attrs,
         receipt_kind,
         receipt_state,
         continuation_id,
         target
       ) do
    LifecycleContinuation.enqueue(%{
      continuation_id: continuation_id,
      tenant_id: execution.tenant_id,
      installation_id: execution.installation_id,
      subject_id: execution.subject_id,
      execution_id: execution.id,
      from_state: Atom.to_string(execution.dispatch_state),
      target_transition: target_transition(target),
      trace_id: execution.trace_id,
      status: :pending,
      actor_ref: required_map!(attrs, :actor_ref),
      metadata: %{
        "effect_ref" => map_value(execution.intent_snapshot, :effect_ref),
        "receipt_ref" => required!(attrs, :receipt_ref),
        "receipt_kind" => Atom.to_string(receipt_kind),
        "receipt_state" => receipt_state,
        "causation_id" => required!(attrs, :causation_id),
        "compensation_ref" =>
          "compensation:governed-effect:#{map_value(execution.intent_snapshot, :effect_ref)}",
        "authority_decision_ref" => map_value(execution.intent_snapshot, :decision_ref),
        "continuation_target" => target,
        "retry_posture" =>
          if(receipt_kind == :ambiguous,
            do: "reconciliation_only_effect_retry_prohibited",
            else: "owner_command"
          )
      }
    })
  end

  defp replay_terminal(execution, receipt_ref) do
    if map_value(execution.lower_receipt, :receipt_ref) == receipt_ref do
      with {:ok, effect_record} <- to_effect_record(execution) do
        {:ok, %{status: :reused, execution: execution, effect_record: effect_record}}
      end
    else
      {:error, :terminal_effect_receipt_conflict}
    end
  end

  defp to_effect_record(execution) do
    intent = execution.intent_snapshot
    receipt = execution.lower_receipt
    status = effect_status(execution.dispatch_state)

    EffectRecord.new(%{
      contract_version: map_value(intent, :contract_version) || 1,
      effect_ref: map_value(intent, :effect_ref),
      run_ref: map_value(intent, :run_ref),
      turn_ref: map_value(intent, :turn_ref),
      command_ref: map_value(intent, :command_ref),
      decision_ref: map_value(intent, :decision_ref),
      grant_ref: map_value(intent, :grant_ref),
      review_ref: map_value(intent, :review_ref),
      idempotency_key: execution.submission_dedupe_key,
      target_ref: map_value(intent, :target_ref),
      status: status,
      row_version: execution.row_version,
      attempt_ref: runtime_ref(status, effect_attempt_ref(execution)),
      execution_ref: execution_runtime_ref(status, execution),
      external_ref: runtime_ref(status, external_ref(execution)),
      receipt_ref: terminal_ref(status, map_value(receipt, :receipt_ref)),
      ambiguity_state:
        if(status == "ambiguous", do: map_value(receipt, :ambiguity_state), else: nil),
      result_artifact_ref:
        if(status == "completed", do: map_value(receipt, :result_artifact_ref), else: nil)
    })
  end

  defp effect_status(:queued), do: "authorized"
  defp effect_status(:in_flight), do: "dispatching"
  defp effect_status(:accepted_active), do: "running"
  defp effect_status(:completed), do: "completed"
  defp effect_status(:cancelled), do: "cancelled"
  defp effect_status(:stalled), do: "ambiguous"
  defp effect_status(state) when state in [:failed, :rejected], do: "failed"

  defp runtime_ref("authorized", _ref), do: nil
  defp runtime_ref(_status, ref), do: ref

  defp execution_runtime_ref(status, _execution) when status in ["authorized", "dispatching"],
    do: nil

  defp execution_runtime_ref(_status, execution), do: execution_ref(execution)

  defp terminal_ref(status, ref)
       when status in ["completed", "failed", "cancelled", "ambiguous"],
       do: ref

  defp terminal_ref(_status, _ref), do: nil

  defp effect_attempt_ref(execution),
    do:
      map_value(execution.lower_receipt, :attempt_ref) ||
        map_value(execution.submission_ref, :attempt_ref) ||
        map_value(execution.dispatch_envelope, :attempt_ref)

  defp execution_ref(execution), do: "execution://#{execution.id}"

  defp external_ref(execution),
    do:
      map_value(execution.lower_receipt, :external_ref) ||
        map_value(execution.submission_ref, :external_ref)

  defp existing_execution(installation_id, idempotency_key) do
    ExecutionRecord
    |> Ash.Query.filter(
      installation_id == ^installation_id and submission_dedupe_key == ^idempotency_key
    )
    |> Ash.read(authorize?: false, domain: Mezzanine.Execution)
    |> case do
      {:ok, [execution | _]} -> {:ok, execution}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_execution(%ExecutionRecord{} = execution), do: {:ok, execution}

  defp load_execution(execution_id) when is_binary(execution_id) do
    case Ash.get(ExecutionRecord, execution_id, authorize?: false, domain: Mezzanine.Execution) do
      {:ok, %ExecutionRecord{} = execution} -> {:ok, execution}
      {:ok, nil} -> {:error, :effect_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp load_execution(_execution), do: {:error, :effect_not_found}

  defp expected_version(execution, attrs) do
    if value(attrs, :expected_row_version) == execution.row_version,
      do: :ok,
      else: {:error, :stale_effect_record}
  end

  defp expected_version_or_terminal_replay(execution, attrs) do
    if execution.dispatch_state in @terminal_states,
      do: :ok,
      else: expected_version(execution, attrs)
  end

  defp ensure_state(execution, states) do
    if execution.dispatch_state in states,
      do: :ok,
      else: {:error, {:invalid_effect_state, execution.dispatch_state}}
  end

  defp artifact_refs(attrs, required_ref \\ nil) do
    attrs
    |> value(:artifact_refs)
    |> List.wrap()
    |> maybe_prepend(required_ref)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp maybe_prepend(values, nil), do: values
  defp maybe_prepend(values, value), do: [value | values]

  defp normalize_failure_kind(nil), do: :fatal_error
  defp normalize_failure_kind(kind) when is_atom(kind), do: kind
  defp normalize_failure_kind("transient_failure"), do: :transient_failure
  defp normalize_failure_kind("timeout"), do: :timeout
  defp normalize_failure_kind("infrastructure_error"), do: :infrastructure_error
  defp normalize_failure_kind("auth_error"), do: :auth_error
  defp normalize_failure_kind("semantic_failure"), do: :semantic_failure
  defp normalize_failure_kind("fatal_error"), do: :fatal_error
  defp normalize_failure_kind(_kind), do: :fatal_error

  defp target_transition(%{"kind" => "owner_command", "command" => command}), do: command
  defp target_transition(%{"kind" => "workflow_signal", "signal" => signal}), do: signal

  defp require_fields(attrs, fields) do
    missing = Enum.reject(fields, &present?(value(attrs, &1)))
    if missing == [], do: :ok, else: {:error, {:missing_governed_effect_fields, missing}}
  end

  defp valid_uuid_field(attrs, key) do
    case Ecto.UUID.cast(value(attrs, key)) do
      {:ok, _uuid} -> :ok
      :error -> {:error, {:invalid_uuid, key}}
    end
  end

  defp reject_forbidden_keys(value) do
    case find_forbidden_key(value) do
      nil -> :ok
      key -> {:error, {:forbidden_governed_effect_field, key}}
    end
  end

  defp find_forbidden_key(%_{} = struct), do: struct |> Map.from_struct() |> find_forbidden_key()

  defp find_forbidden_key(map) when is_map(map) do
    Enum.find_value(map, fn {key, nested} ->
      normalized = key |> to_string() |> String.downcase()
      if normalized in @forbidden_keys, do: normalized, else: find_forbidden_key(nested)
    end)
  end

  defp find_forbidden_key(values) when is_list(values),
    do: Enum.find_value(values, &find_forbidden_key/1)

  defp find_forbidden_key(_value), do: nil

  defp relative_file?(path) when is_binary(path) do
    String.trim(path) != "" and Path.type(path) == :relative and
      path |> Path.split() |> Enum.all?(&(&1 not in [".", ".."]))
  end

  defp relative_file?(_path), do: false

  defp sha256?(<<"sha256:", digest::binary-size(64)>>),
    do: String.match?(digest, ~r/\A[0-9a-f]{64}\z/)

  defp sha256?(_value), do: false

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp normalize_attrs(attrs) do
    case Support.normalize_attrs(attrs) do
      {:ok, attrs} -> {:ok, attrs}
      {:error, _reason} -> {:error, :invalid_governed_effect_command}
    end
  end

  defp required!(attrs, key) do
    case value(attrs, key) do
      nil -> raise ArgumentError, "missing governed effect field #{inspect(key)}"
      nested -> nested
    end
  end

  defp required_map!(attrs, key) do
    case required!(attrs, key) do
      %{} = map -> map
      _value -> raise ArgumentError, "invalid governed effect map #{inspect(key)}"
    end
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value) when is_list(value), do: value != []
  defp present?(nil), do: false
  defp present?(_value), do: true

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp stringify(%_{} = struct), do: struct |> Map.from_struct() |> stringify()

  defp stringify(map) when is_map(map),
    do: Map.new(map, fn {key, nested} -> {to_string(key), stringify(nested)} end)

  defp stringify(values) when is_list(values), do: Enum.map(values, &stringify/1)
  defp stringify(value) when is_atom(value) and not is_boolean(value), do: Atom.to_string(value)
  defp stringify(value), do: value

  defp value(nil, _key), do: nil
  defp value(map, key) when is_map(map), do: Map.get(map, key, Map.get(map, to_string(key)))
  defp value(_value, _key), do: nil
  defp map_value(map, key), do: value(map, key)
end
