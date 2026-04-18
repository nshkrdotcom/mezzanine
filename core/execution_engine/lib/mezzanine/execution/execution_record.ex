defmodule Mezzanine.Execution.ExecutionRecord do
  @moduledoc """
  Durable substrate execution ledger row owned by JobOutbox-backed dispatch
  workers.
  """

  use Ash.Resource,
    domain: Mezzanine.Execution,
    data_layer: AshPostgres.DataLayer

  alias Mezzanine.Audit.{AuditFact, ExecutionLineage, ExecutionLineageStore}
  alias Mezzanine.Execution.Repo
  alias Mezzanine.JobOutbox

  @active_dispatch_states [
    :pending_dispatch,
    :dispatching,
    :dispatching_retry,
    :awaiting_receipt,
    :running
  ]
  @failure_kinds [
    :transient_failure,
    :timeout,
    :infrastructure_error,
    :auth_error,
    :semantic_failure,
    :fatal_error
  ]
  @supersession_reasons [
    :retry_transient,
    :retry_semantic,
    :operator_replan,
    :pack_revision_change,
    :manual_retry
  ]
  @default_dispatch_queue :dispatch

  postgres do
    table("execution_records")
    repo(Mezzanine.Execution.Repo)

    custom_indexes do
      index([:installation_id, :trace_id])
      index([:causation_id])
      index([:subject_id, :dispatch_state])
      index([:barrier_id])
      index([:next_dispatch_at])
      index([:installation_id, :submission_dedupe_key], unique: true)
    end
  end

  code_interface do
    define(:record_accepted, action: :record_accepted)
    define(:record_retryable_failure, action: :record_retryable_failure)
    define(:record_restart_recovery, action: :record_restart_recovery)
    define(:record_completed, action: :record_completed)
    define(:record_failed_outcome, action: :record_failed_outcome)
    define(:record_cancelled_outcome, action: :record_cancelled_outcome)
    define(:record_operator_cancelled, action: :record_operator_cancelled)
    define(:record_terminal_rejection, action: :record_terminal_rejection)
    define(:record_lookup_expired, action: :record_lookup_expired)
    define(:record_semantic_failure, action: :record_semantic_failure)
    define(:active_for_subject, action: :active_for_subject, args: [:subject_id])
  end

  actions do
    defaults([:read])

    create :create_dispatch_record do
      accept([
        :tenant_id,
        :installation_id,
        :subject_id,
        :barrier_id,
        :recipe_ref,
        :compiled_pack_revision,
        :binding_snapshot,
        :dispatch_envelope,
        :intent_snapshot,
        :submission_dedupe_key,
        :trace_id,
        :causation_id,
        :supersedes_execution_id,
        :supersession_reason,
        :supersession_depth
      ])

      change(set_attribute(:dispatch_state, :pending_dispatch))
      change(set_attribute(:dispatch_attempt_count, 0))
      change(set_attribute(:next_dispatch_at, &DateTime.utc_now/0, set_when_nil?: false))
    end

    update :mark_dispatching do
      accept([])
      require_atomic?(false)

      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :dispatching))
      change(set_attribute(:causation_id, arg(:causation_id)))
    end

    update :record_accepted do
      accept([])
      require_atomic?(false)

      argument(:submission_ref, :map, allow_nil?: false)
      argument(:lower_receipt, :map, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :awaiting_receipt))
      change(set_attribute(:submission_ref, arg(:submission_ref)))
      change(set_attribute(:lower_receipt, arg(:lower_receipt)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:next_dispatch_at, nil))
      change(set_attribute(:last_dispatch_error_kind, nil))
      change(set_attribute(:last_dispatch_error_payload, %{}))
      change(set_attribute(:terminal_rejection_reason, nil))
      change(set_attribute(:failure_kind, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _lineage} <- store_lineage_update(execution),
               {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_dispatched,
                   %{
                     classification: "accepted",
                     submission_ref: execution.submission_ref
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_retryable_failure do
      accept([])
      require_atomic?(false)

      argument(:last_dispatch_error_kind, :string, allow_nil?: false)
      argument(:last_dispatch_error_payload, :map, allow_nil?: false)
      argument(:next_dispatch_at, :utc_datetime_usec, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :dispatching_retry))
      change(increment(:dispatch_attempt_count))
      change(set_attribute(:next_dispatch_at, arg(:next_dispatch_at)))
      change(set_attribute(:last_dispatch_error_kind, arg(:last_dispatch_error_kind)))
      change(set_attribute(:last_dispatch_error_payload, arg(:last_dispatch_error_payload)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:failure_kind, nil))
      change(set_attribute(:terminal_rejection_reason, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_failed,
                   %{
                     classification: "retryable_failure",
                     error_kind: execution.last_dispatch_error_kind
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_terminal_rejection do
      accept([])
      require_atomic?(false)

      argument(:terminal_rejection_reason, :string, allow_nil?: false)
      argument(:last_dispatch_error_payload, :map, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :rejected))
      change(set_attribute(:terminal_rejection_reason, arg(:terminal_rejection_reason)))
      change(set_attribute(:last_dispatch_error_kind, "terminal_rejection"))
      change(set_attribute(:last_dispatch_error_payload, arg(:last_dispatch_error_payload)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:next_dispatch_at, nil))
      change(set_attribute(:failure_kind, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_failed,
                   %{
                     classification: "terminal_rejection",
                     terminal_rejection_reason: execution.terminal_rejection_reason
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_completed do
      accept([])
      require_atomic?(false)

      argument(:receipt_id, :string, allow_nil?: false)
      argument(:lower_receipt, :map, allow_nil?: false)
      argument(:normalized_outcome, :map, allow_nil?: false)
      argument(:artifact_refs, {:array, :string}, allow_nil?: false, default: [])
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :completed))
      change(set_attribute(:lower_receipt, arg(:lower_receipt)))
      change(set_attribute(:last_dispatch_error_kind, nil))
      change(set_attribute(:last_dispatch_error_payload, %{}))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:next_dispatch_at, nil))
      change(set_attribute(:terminal_rejection_reason, nil))
      change(set_attribute(:failure_kind, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _lineage} <-
                 store_lineage_update(
                   execution,
                   artifact_refs: Ash.Changeset.get_argument(changeset, :artifact_refs)
                 ),
               {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_completed,
                   %{
                     receipt_id: Ash.Changeset.get_argument(changeset, :receipt_id),
                     normalized_outcome:
                       Ash.Changeset.get_argument(changeset, :normalized_outcome)
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_failed_outcome do
      accept([])
      require_atomic?(false)

      argument(:receipt_id, :string, allow_nil?: false)
      argument(:failure_kind, :atom, allow_nil?: false)
      argument(:lower_receipt, :map, allow_nil?: false)
      argument(:normalized_outcome, :map, allow_nil?: false)
      argument(:artifact_refs, {:array, :string}, allow_nil?: false, default: [])
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :failed))
      change(set_attribute(:failure_kind, arg(:failure_kind)))
      change(set_attribute(:lower_receipt, arg(:lower_receipt)))
      change(set_attribute(:last_dispatch_error_kind, "execution_failed"))
      change(set_attribute(:last_dispatch_error_payload, arg(:normalized_outcome)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:next_dispatch_at, nil))
      change(set_attribute(:terminal_rejection_reason, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _lineage} <-
                 store_lineage_update(
                   execution,
                   artifact_refs: Ash.Changeset.get_argument(changeset, :artifact_refs)
                 ),
               {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_failed,
                   %{
                     classification: "outcome_failure",
                     receipt_id: Ash.Changeset.get_argument(changeset, :receipt_id),
                     failure_kind: Atom.to_string(execution.failure_kind),
                     normalized_outcome:
                       Ash.Changeset.get_argument(changeset, :normalized_outcome)
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_cancelled_outcome do
      accept([])
      require_atomic?(false)

      argument(:receipt_id, :string, allow_nil?: false)
      argument(:lower_receipt, :map, allow_nil?: false)
      argument(:normalized_outcome, :map, allow_nil?: false)
      argument(:artifact_refs, {:array, :string}, allow_nil?: false, default: [])
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :cancelled))
      change(set_attribute(:lower_receipt, arg(:lower_receipt)))
      change(set_attribute(:last_dispatch_error_kind, "execution_cancelled"))
      change(set_attribute(:last_dispatch_error_payload, arg(:normalized_outcome)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:next_dispatch_at, nil))
      change(set_attribute(:terminal_rejection_reason, nil))
      change(set_attribute(:failure_kind, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _lineage} <-
                 store_lineage_update(
                   execution,
                   artifact_refs: Ash.Changeset.get_argument(changeset, :artifact_refs)
                 ),
               {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_cancelled,
                   %{
                     receipt_id: Ash.Changeset.get_argument(changeset, :receipt_id),
                     normalized_outcome:
                       Ash.Changeset.get_argument(changeset, :normalized_outcome)
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_operator_cancelled do
      accept([])
      require_atomic?(false)

      argument(:reason, :string)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :cancelled))
      change(set_attribute(:last_dispatch_error_kind, "operator_cancelled"))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:next_dispatch_at, nil))
      change(set_attribute(:terminal_rejection_reason, nil))
      change(set_attribute(:failure_kind, nil))

      change(fn changeset, _context ->
        Ash.Changeset.change_attribute(changeset, :last_dispatch_error_payload, %{
          "reason" => Ash.Changeset.get_argument(changeset, :reason)
        })
      end)

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_cancelled,
                   %{
                     classification: "operator_cancelled",
                     reason: Ash.Changeset.get_argument(changeset, :reason)
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_restart_recovery do
      accept([])
      require_atomic?(false)

      argument(:next_dispatch_at, :utc_datetime_usec, allow_nil?: false)
      argument(:last_dispatch_error_payload, :map, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :dispatching_retry))
      change(increment(:dispatch_attempt_count))
      change(set_attribute(:next_dispatch_at, arg(:next_dispatch_at)))
      change(set_attribute(:last_dispatch_error_kind, "restart_recovery"))
      change(set_attribute(:last_dispatch_error_payload, arg(:last_dispatch_error_payload)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:failure_kind, nil))
      change(set_attribute(:terminal_rejection_reason, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_recovered,
                   %{
                     classification: "restart_recovery",
                     recovered_from: "dispatching"
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_lookup_expired do
      accept([])
      require_atomic?(false)

      argument(:last_dispatch_error_payload, :map, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :failed))
      change(set_attribute(:failure_kind, :infrastructure_error))
      change(set_attribute(:last_dispatch_error_kind, "submission_lookup_expired"))
      change(set_attribute(:last_dispatch_error_payload, arg(:last_dispatch_error_payload)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:next_dispatch_at, nil))
      change(set_attribute(:terminal_rejection_reason, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_failed,
                   %{
                     classification: "submission_lookup_expired",
                     failure_kind: "infrastructure_error"
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    update :record_semantic_failure do
      accept([])
      require_atomic?(false)

      argument(:lower_receipt, :map, allow_nil?: false)
      argument(:last_dispatch_error_payload, :map, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:dispatch_state, :failed))
      change(set_attribute(:failure_kind, :semantic_failure))
      change(set_attribute(:lower_receipt, arg(:lower_receipt)))
      change(set_attribute(:last_dispatch_error_kind, "semantic_failure"))
      change(set_attribute(:last_dispatch_error_payload, arg(:last_dispatch_error_payload)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:next_dispatch_at, nil))
      change(set_attribute(:terminal_rejection_reason, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_failed,
                   %{
                     classification: "semantic_failure",
                     failure_kind: "semantic_failure"
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
    end

    read :active_for_subject do
      argument(:subject_id, :uuid, allow_nil?: false)
      filter(expr(subject_id == ^arg(:subject_id) and dispatch_state in ^@active_dispatch_states))
      prepare(build(limit: 1))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :tenant_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :installation_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :subject_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :barrier_id, :uuid do
      public?(true)
    end

    attribute :recipe_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :compiled_pack_revision, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
    end

    attribute :binding_snapshot, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :dispatch_envelope, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :intent_snapshot, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :submission_dedupe_key, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :trace_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :causation_id, :string do
      public?(true)
    end

    attribute :dispatch_state, :atom do
      allow_nil?(false)

      constraints(
        one_of: [
          :pending_dispatch,
          :dispatching,
          :dispatching_retry,
          :awaiting_receipt,
          :running,
          :completed,
          :cancelled,
          :failed,
          :rejected,
          :stalled
        ]
      )

      public?(true)
    end

    attribute :dispatch_attempt_count, :integer do
      allow_nil?(false)
      default(0)
      public?(true)
    end

    attribute :next_dispatch_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :submission_ref, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :lower_receipt, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :last_dispatch_error_kind, :string do
      public?(true)
    end

    attribute :last_dispatch_error_payload, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :terminal_rejection_reason, :string do
      public?(true)
    end

    attribute :failure_kind, :atom do
      constraints(one_of: @failure_kinds)
      public?(true)
    end

    attribute :last_reconcile_wave_id, :string do
      public?(true)
    end

    attribute :supersedes_execution_id, :uuid do
      public?(true)
    end

    attribute :supersession_reason, :atom do
      constraints(one_of: @supersession_reasons)
      public?(true)
    end

    attribute :supersession_depth, :integer do
      allow_nil?(false)
      default(0)
      constraints(min: 0)
      public?(true)
    end

    attribute :row_version, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
      writable?(false)
    end

    timestamps()
  end

  identities do
    identity(:unique_submission_dedupe_key, [:installation_id, :submission_dedupe_key])
  end

  @spec dispatch(map()) :: {:ok, t()} | {:error, term()}
  def dispatch(attrs) when is_map(attrs) do
    attrs = Map.new(attrs)
    actor_ref = fetch_required!(attrs, :actor_ref)
    create_attrs = Map.drop(attrs, [:actor_ref, "actor_ref"])

    case create_dispatch_record_and_enqueue(create_attrs) do
      {:ok, {execution, notifications}} ->
        Ash.Notifier.notify(notifications)

        with {:ok, _lineage} <- store_lineage(execution),
             {:ok, _fact} <- record_dispatch_audit_fact(execution, actor_ref) do
          {:ok, execution}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  @spec mark_dispatching(t() | Ecto.UUID.t(), map()) :: {:ok, t()} | {:error, term()}
  def mark_dispatching(execution_or_id, attrs) when is_map(attrs) do
    with {:ok, execution} <- load_execution(execution_or_id),
         changeset <- Ash.Changeset.for_update(execution, :mark_dispatching, attrs),
         {:ok, marked_execution, notifications} <- update_execution(changeset) do
      Ash.Notifier.notify(notifications)
      {:ok, marked_execution}
    end
  end

  @spec enqueue_dispatch(t() | Ecto.UUID.t(), keyword()) ::
          {:ok, JobOutbox.job_ref()} | {:error, term()}
  def enqueue_dispatch(execution_or_id, opts \\ []) do
    with {:ok, execution} <- load_execution(execution_or_id) do
      scheduled_at =
        Keyword.get(opts, :scheduled_at, execution.next_dispatch_at || DateTime.utc_now())

      JobOutbox.enqueue(
        Keyword.get(opts, :queue, @default_dispatch_queue),
        Mezzanine.ExecutionDispatchWorker,
        %{execution_id: execution.id},
        scheduled_at: scheduled_at
      )
    end
  end

  @spec has_active_execution?(Ecto.UUID.t()) :: boolean()
  def has_active_execution?(subject_id) do
    case active_for_subject(subject_id) do
      {:ok, []} -> false
      {:ok, [_ | _]} -> true
      _ -> false
    end
  end

  defp load_execution(%{id: _id} = execution), do: {:ok, execution}

  defp load_execution(execution_id) when is_binary(execution_id),
    do: Ash.get(__MODULE__, execution_id)

  defp fetch_required!(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} ->
        value

      :error ->
        Map.fetch!(attrs, Atom.to_string(key))
    end
  end

  defp store_lineage(execution) do
    %{
      trace_id: execution.trace_id,
      causation_id: execution.causation_id,
      installation_id: execution.installation_id,
      subject_id: execution.subject_id,
      execution_id: execution.id
    }
    |> ExecutionLineage.new!()
    |> ExecutionLineageStore.store()
  end

  defp store_lineage_update(execution, opts \\ []) do
    artifact_refs = Keyword.get(opts, :artifact_refs, [])

    case ExecutionLineageStore.fetch(execution.id) do
      {:ok, current_lineage} ->
        current_lineage
        |> Map.from_struct()
        |> Map.merge(%{
          trace_id: execution.trace_id,
          causation_id: execution.causation_id,
          installation_id: execution.installation_id,
          subject_id: execution.subject_id,
          execution_id: execution.id,
          ji_submission_key: get_in(execution.lower_receipt, ["ji_submission_key"]),
          lower_run_id: get_in(execution.lower_receipt, ["run_id"]),
          lower_attempt_id: get_in(execution.lower_receipt, ["attempt_id"]),
          artifact_refs: artifact_refs
        })
        |> ExecutionLineage.new!()
        |> ExecutionLineageStore.store()

      {:error, error} ->
        if lineage_missing?(error) do
          execution
          |> seed_lineage()
          |> Map.merge(%{
            ji_submission_key: get_in(execution.lower_receipt, ["ji_submission_key"]),
            lower_run_id: get_in(execution.lower_receipt, ["run_id"]),
            lower_attempt_id: get_in(execution.lower_receipt, ["attempt_id"]),
            artifact_refs: artifact_refs
          })
          |> ExecutionLineage.new!()
          |> ExecutionLineageStore.store()
        else
          {:error, error}
        end
    end
  end

  defp seed_lineage(execution) do
    %{
      trace_id: execution.trace_id,
      causation_id: execution.causation_id,
      installation_id: execution.installation_id,
      subject_id: execution.subject_id,
      execution_id: execution.id,
      citadel_request_id: nil,
      citadel_submission_id: nil,
      ji_submission_key: nil,
      lower_run_id: nil,
      lower_attempt_id: nil,
      artifact_refs: []
    }
  end

  defp lineage_missing?(%{errors: errors}) when is_list(errors) do
    Enum.any?(errors, &match?(%Ash.Error.Query.NotFound{}, &1))
  end

  defp lineage_missing?(_error), do: false

  defp record_audit_fact(execution, actor_ref, fact_kind, payload) do
    AuditFact.record(%{
      installation_id: execution.installation_id,
      subject_id: execution.subject_id,
      execution_id: execution.id,
      trace_id: execution.trace_id,
      causation_id: execution.causation_id,
      fact_kind: fact_kind,
      actor_ref: actor_ref,
      payload: payload,
      occurred_at: DateTime.utc_now()
    })
  end

  defp create_dispatch_record_and_enqueue(create_attrs) do
    Repo.transaction(fn ->
      changeset = Ash.Changeset.for_create(__MODULE__, :create_dispatch_record, create_attrs)

      with {:ok, execution, notifications} <- create_dispatch_record(changeset),
           {:ok, _job_ref} <- enqueue_dispatch(execution) do
        {execution, notifications}
      else
        {:error, error} -> Repo.rollback(error)
      end
    end)
  end

  defp create_dispatch_record(changeset) do
    Ash.create(
      changeset,
      authorize?: false,
      domain: Mezzanine.Execution,
      return_notifications?: true
    )
  end

  defp update_execution(changeset) do
    Ash.update(
      changeset,
      authorize?: false,
      domain: Mezzanine.Execution,
      return_notifications?: true
    )
  end

  defp record_dispatch_audit_fact(execution, actor_ref) do
    record_audit_fact(
      execution,
      actor_ref,
      :execution_dispatched,
      %{
        recipe_ref: execution.recipe_ref,
        dispatch_state: execution.dispatch_state,
        submission_dedupe_key: execution.submission_dedupe_key
      }
    )
  end
end
