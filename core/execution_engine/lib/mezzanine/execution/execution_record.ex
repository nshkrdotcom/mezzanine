defmodule Mezzanine.Execution.ExecutionRecord do
  @moduledoc """
  Durable substrate execution ledger row with explicit dispatch-state ownership.
  """

  use Ash.Resource,
    domain: Mezzanine.Execution,
    data_layer: AshPostgres.DataLayer

  alias Mezzanine.Audit.{AuditFact, ExecutionLineage, ExecutionLineageStore}
  alias Mezzanine.Execution.DispatchOutboxEntry

  @active_dispatch_states [
    :pending_dispatch,
    :dispatching,
    :dispatching_retry,
    :accepted,
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

  postgres do
    table("execution_records")
    repo(Mezzanine.Execution.Repo)

    custom_indexes do
      index([:installation_id, :trace_id])
      index([:causation_id])
      index([:subject_id, :dispatch_state])
      index([:next_dispatch_at])
    end
  end

  code_interface do
    define(:dispatch, action: :dispatch)
    define(:record_accepted, action: :record_accepted)
    define(:record_retryable_failure, action: :record_retryable_failure)
    define(:record_restart_recovery, action: :record_restart_recovery)
    define(:record_terminal_rejection, action: :record_terminal_rejection)
    define(:record_semantic_failure, action: :record_semantic_failure)
    define(:active_for_subject, action: :active_for_subject, args: [:subject_id])
  end

  actions do
    defaults([:read])

    create :dispatch do
      accept([
        :installation_id,
        :subject_id,
        :recipe_ref,
        :compiled_pack_revision,
        :binding_snapshot,
        :trace_id,
        :causation_id
      ])

      argument(:dispatch_envelope, :map, allow_nil?: false)
      argument(:submission_dedupe_key, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(set_attribute(:dispatch_state, :pending_dispatch))
      change(set_attribute(:dispatch_attempt_count, 0))
      change(set_attribute(:next_dispatch_at, &DateTime.utc_now/0, set_when_nil?: false))

      change(
        after_action(fn changeset, execution, _context ->
          outbox_attrs = %{
            execution_id: execution.id,
            installation_id: execution.installation_id,
            subject_id: execution.subject_id,
            trace_id: execution.trace_id,
            causation_id: execution.causation_id,
            status: :pending,
            dispatch_envelope: Ash.Changeset.get_argument(changeset, :dispatch_envelope),
            submission_dedupe_key: Ash.Changeset.get_argument(changeset, :submission_dedupe_key),
            compiled_pack_revision: execution.compiled_pack_revision,
            binding_snapshot: execution.binding_snapshot,
            available_at: execution.next_dispatch_at
          }

          with {:ok, outbox} <- DispatchOutboxEntry.enqueue(outbox_attrs),
               {:ok, _lineage} <- store_lineage(execution, outbox),
               {:ok, _fact} <-
                 record_audit_fact(
                   execution,
                   Ash.Changeset.get_argument(changeset, :actor_ref),
                   :execution_dispatched,
                   %{
                     recipe_ref: execution.recipe_ref,
                     dispatch_state: execution.dispatch_state,
                     outbox_id: outbox.id,
                     outbox_status: outbox.status
                   }
                 ) do
            {:ok, execution}
          end
        end)
      )
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
      change(set_attribute(:dispatch_state, :accepted))
      change(set_attribute(:submission_ref, arg(:submission_ref)))
      change(set_attribute(:lower_receipt, arg(:lower_receipt)))
      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:last_dispatch_error_kind, nil))
      change(set_attribute(:last_dispatch_error_payload, %{}))
      change(set_attribute(:terminal_rejection_reason, nil))
      change(set_attribute(:failure_kind, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, outbox} <- DispatchOutboxEntry.by_execution_id(execution.id),
               {:ok, _outbox} <- DispatchOutboxEntry.mark_completed(outbox, %{}),
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
      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:failure_kind, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, outbox} <- DispatchOutboxEntry.by_execution_id(execution.id),
               {:ok, _outbox} <-
                 DispatchOutboxEntry.mark_pending_retry(outbox, %{
                   available_at: execution.next_dispatch_at,
                   last_error_kind: execution.last_dispatch_error_kind,
                   last_error_payload: execution.last_dispatch_error_payload
                 }),
               {:ok, _fact} <-
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
      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:failure_kind, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, outbox} <- DispatchOutboxEntry.by_execution_id(execution.id),
               {:ok, _outbox} <-
                 DispatchOutboxEntry.mark_terminal(outbox, %{
                   last_error_kind: execution.last_dispatch_error_kind,
                   last_error_payload: execution.last_dispatch_error_payload
                 }),
               {:ok, _fact} <-
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
      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:failure_kind, nil))
      change(set_attribute(:terminal_rejection_reason, nil))

      change(
        after_action(fn changeset, execution, _context ->
          with {:ok, outbox} <- DispatchOutboxEntry.by_execution_id(execution.id),
               :ok <- ensure_dispatching_outbox(outbox),
               {:ok, _outbox} <-
                 DispatchOutboxEntry.mark_pending_retry(outbox, %{
                   available_at: execution.next_dispatch_at,
                   last_error_kind: execution.last_dispatch_error_kind,
                   last_error_payload: execution.last_dispatch_error_payload
                 }),
               {:ok, _fact} <-
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
      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))
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

    attribute :installation_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :subject_id, :uuid do
      allow_nil?(false)
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
          :accepted,
          :running,
          :completed,
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

    attribute :row_version, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
      writable?(false)
    end

    timestamps()
  end

  @spec has_active_execution?(Ecto.UUID.t()) :: boolean()
  def has_active_execution?(subject_id) do
    case active_for_subject(subject_id) do
      {:ok, []} -> false
      {:ok, [_ | _]} -> true
      _ -> false
    end
  end

  defp store_lineage(execution, outbox) do
    %{
      trace_id: execution.trace_id,
      causation_id: execution.causation_id,
      installation_id: execution.installation_id,
      subject_id: execution.subject_id,
      execution_id: execution.id,
      dispatch_outbox_entry_id: outbox.id
    }
    |> ExecutionLineage.new!()
    |> ExecutionLineageStore.store()
  end

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

  defp ensure_dispatching_outbox(%DispatchOutboxEntry{status: :dispatching}), do: :ok

  defp ensure_dispatching_outbox(%DispatchOutboxEntry{status: status}),
    do: {:error, {:unexpected_outbox_status, status}}
end
