defmodule Mezzanine.Decisions.DecisionRecord do
  @moduledoc """
  Durable decision ledger row with explicit review-state ownership and SLA reads.
  """

  use Ash.Resource,
    domain: Mezzanine.Decisions,
    data_layer: AshPostgres.DataLayer

  alias Mezzanine.Audit.AuditAppend

  @resolved_states ["resolved", "waived", "expired"]

  postgres do
    table("decision_records")
    repo(Mezzanine.Decisions.Repo)

    custom_indexes do
      index([:installation_id, :trace_id])
      index([:causation_id])
      index([:subject_id, :lifecycle_state])
      index([:installation_id, :required_by])
      index([:installation_id, :subject_id, :decision_kind, :execution_id], unique: true)
    end
  end

  code_interface do
    define(:create_pending, action: :create_pending)
    define(:decide, action: :decide)
    define(:waive, action: :waive)
    define(:escalate, action: :escalate)
    define(:expire, action: :expire)

    define(:by_identity,
      action: :by_identity,
      args: [:installation_id, :subject_id, :execution_id, :decision_kind]
    )

    define(:by_identity_without_execution,
      action: :by_identity_without_execution,
      args: [:installation_id, :subject_id, :decision_kind]
    )

    define(:for_subject_kind, action: :for_subject_kind, args: [:subject_id, :decision_kind])
    define(:resolved_for_subject, action: :resolved_for_subject, args: [:subject_id])
    define(:pending_for_installation, action: :pending_for_installation, args: [:installation_id])
    define(:overdue, action: :overdue, args: [:installation_id, :now])
  end

  actions do
    defaults([:read])

    create :create_pending do
      accept([
        :installation_id,
        :subject_id,
        :execution_id,
        :decision_kind,
        :required_by
      ])

      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(set_attribute(:lifecycle_state, "pending"))
      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))

      change(
        after_action(fn changeset, decision, _context ->
          append_audit_fact(
            decision,
            Ash.Changeset.get_argument(changeset, :actor_ref),
            :decision_created,
            %{
              decision_kind: decision.decision_kind,
              lifecycle_state: decision.lifecycle_state,
              required_by: decision.required_by,
              workflow_timer_ref: workflow_timer_ref(decision)
            }
          )
          |> case do
            {:ok, _fact} -> {:ok, decision}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :decide do
      accept([])
      require_atomic?(false)

      argument(:decision_value, :string, allow_nil?: false)
      argument(:reason, :string)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:lifecycle_state, "resolved"))
      change(set_attribute(:decision_value, arg(:decision_value)))
      change(set_attribute(:reason, arg(:reason)))
      change(set_attribute(:resolved_at, &DateTime.utc_now/0))
      change(set_attribute(:causation_id, arg(:causation_id)))

      change(
        after_action(fn changeset, decision, _context ->
          append_audit_fact(
            decision,
            Ash.Changeset.get_argument(changeset, :actor_ref),
            :decision_resolved,
            %{
              decision_kind: decision.decision_kind,
              lifecycle_state: decision.lifecycle_state,
              decision_value: decision.decision_value,
              reason: decision.reason
            }
          )
          |> case do
            {:ok, _fact} -> {:ok, decision}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :waive do
      accept([])
      require_atomic?(false)

      argument(:reason, :string)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:lifecycle_state, "waived"))
      change(set_attribute(:decision_value, "waive"))
      change(set_attribute(:reason, arg(:reason)))
      change(set_attribute(:resolved_at, &DateTime.utc_now/0))
      change(set_attribute(:causation_id, arg(:causation_id)))

      change(
        after_action(fn changeset, decision, _context ->
          append_audit_fact(
            decision,
            Ash.Changeset.get_argument(changeset, :actor_ref),
            :decision_waived,
            %{
              decision_kind: decision.decision_kind,
              lifecycle_state: decision.lifecycle_state,
              decision_value: decision.decision_value,
              reason: decision.reason
            }
          )
          |> case do
            {:ok, _fact} -> {:ok, decision}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :escalate do
      accept([])
      require_atomic?(false)

      argument(:reason, :string, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:lifecycle_state, "escalated"))
      change(set_attribute(:reason, arg(:reason)))
      change(set_attribute(:causation_id, arg(:causation_id)))

      change(
        after_action(fn changeset, decision, _context ->
          append_audit_fact(
            decision,
            Ash.Changeset.get_argument(changeset, :actor_ref),
            :decision_escalated,
            %{
              decision_kind: decision.decision_kind,
              lifecycle_state: decision.lifecycle_state,
              reason: decision.reason
            }
          )
          |> case do
            {:ok, _fact} -> {:ok, decision}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :expire do
      accept([])
      require_atomic?(false)

      argument(:reason, :string)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:lifecycle_state, "expired"))
      change(set_attribute(:decision_value, "expired"))
      change(set_attribute(:reason, arg(:reason)))
      change(set_attribute(:resolved_at, &DateTime.utc_now/0))
      change(set_attribute(:causation_id, arg(:causation_id)))

      change(
        after_action(fn changeset, decision, _context ->
          append_audit_fact(
            decision,
            Ash.Changeset.get_argument(changeset, :actor_ref),
            :decision_expired,
            %{
              decision_kind: decision.decision_kind,
              lifecycle_state: decision.lifecycle_state,
              decision_value: decision.decision_value,
              reason: decision.reason
            }
          )
          |> case do
            {:ok, _fact} -> {:ok, decision}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    read :for_subject_kind do
      argument(:subject_id, :uuid, allow_nil?: false)
      argument(:decision_kind, :string, allow_nil?: false)
      filter(expr(subject_id == ^arg(:subject_id) and decision_kind == ^arg(:decision_kind)))
      prepare(build(sort: [inserted_at: :desc], limit: 1))
    end

    read :by_identity do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:subject_id, :uuid, allow_nil?: false)
      argument(:execution_id, :uuid, allow_nil?: false)
      argument(:decision_kind, :string, allow_nil?: false)

      filter(
        expr(
          installation_id == ^arg(:installation_id) and subject_id == ^arg(:subject_id) and
            execution_id == ^arg(:execution_id) and decision_kind == ^arg(:decision_kind)
        )
      )

      prepare(build(limit: 1))
    end

    read :by_identity_without_execution do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:subject_id, :uuid, allow_nil?: false)
      argument(:decision_kind, :string, allow_nil?: false)

      filter(
        expr(
          installation_id == ^arg(:installation_id) and subject_id == ^arg(:subject_id) and
            is_nil(execution_id) and decision_kind == ^arg(:decision_kind)
        )
      )

      prepare(build(limit: 1))
    end

    read :resolved_for_subject do
      argument(:subject_id, :uuid, allow_nil?: false)
      filter(expr(subject_id == ^arg(:subject_id) and lifecycle_state in ^@resolved_states))
      prepare(build(sort: [updated_at: :desc, inserted_at: :desc]))
    end

    read :pending_for_installation do
      argument(:installation_id, :string, allow_nil?: false)
      filter(expr(installation_id == ^arg(:installation_id) and lifecycle_state == "pending"))
      prepare(build(sort: [required_by: :asc, inserted_at: :asc]))
    end

    read :overdue do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:now, :utc_datetime_usec, allow_nil?: false)

      filter(
        expr(
          installation_id == ^arg(:installation_id) and lifecycle_state == "pending" and
            not is_nil(required_by) and required_by < ^arg(:now)
        )
      )

      prepare(build(sort: [required_by: :asc, inserted_at: :asc]))
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

    attribute :execution_id, :uuid do
      public?(true)
    end

    attribute :decision_kind, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :lifecycle_state, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :decision_value, :string do
      public?(true)
    end

    attribute :required_by, :utc_datetime_usec do
      public?(true)
    end

    attribute :resolved_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :reason, :string do
      public?(true)
    end

    attribute :trace_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :causation_id, :string do
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

  @spec exists?(Ecto.UUID.t(), String.t()) :: boolean()
  def exists?(subject_id, decision_kind) do
    case for_subject_kind(subject_id, decision_kind) do
      {:ok, []} -> false
      {:ok, [_decision | _rest]} -> true
      _ -> false
    end
  end

  @spec read_overdue(String.t(), DateTime.t()) :: {:ok, [struct()]} | {:error, term()}
  def read_overdue(installation_id, now \\ DateTime.utc_now()) do
    overdue(installation_id, now)
  end

  defp append_audit_fact(decision, actor_ref, fact_kind, payload) do
    AuditAppend.append_fact(
      %{
        installation_id: decision.installation_id,
        subject_id: decision.subject_id,
        execution_id: decision.execution_id,
        decision_id: decision.id,
        trace_id: decision.trace_id,
        causation_id: decision.causation_id,
        fact_kind: fact_kind,
        actor_ref: actor_ref,
        payload: payload,
        occurred_at: DateTime.utc_now()
      },
      []
    )
  end

  defp workflow_timer_ref(%{required_by: nil}), do: nil

  defp workflow_timer_ref(%{id: id, required_by: %DateTime{} = required_by}) do
    "workflow-timer://decision/#{id}/#{DateTime.to_unix(required_by, :millisecond)}"
  end
end
