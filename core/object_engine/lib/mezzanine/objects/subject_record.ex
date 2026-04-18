defmodule Mezzanine.Objects.SubjectRecord do
  @moduledoc """
  Durable substrate-owned subject ledger row with canonical lifecycle ownership.
  """

  use Ash.Resource,
    domain: Mezzanine.Objects,
    data_layer: AshPostgres.DataLayer

  alias Mezzanine.Audit.AuditFact

  postgres do
    table("subject_records")
    repo(Mezzanine.Objects.Repo)

    custom_indexes do
      index([:installation_id, :source_ref], unique: true)
      index([:installation_id, :lifecycle_state])
      index([:installation_id, :subject_kind])
      index([:installation_id, :status])
    end
  end

  code_interface do
    define(:ingest, action: :ingest)

    define(:by_installation_source_ref,
      action: :by_installation_source_ref,
      args: [:installation_id, :source_ref]
    )

    define(:advance_lifecycle, action: :advance_lifecycle)
    define(:block, action: :block)
    define(:unblock, action: :unblock)
    define(:pause, action: :pause)
    define(:resume, action: :resume)
    define(:cancel, action: :cancel)
  end

  actions do
    defaults([:read])

    create :ingest do
      accept([
        :installation_id,
        :source_ref,
        :subject_kind,
        :lifecycle_state,
        :status,
        :title,
        :description,
        :schema_ref,
        :schema_version,
        :payload,
        :opened_at
      ])

      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(set_attribute(:opened_at, &DateTime.utc_now/0, set_when_nil?: false))
      change(set_attribute(:status, "active", set_when_nil?: false))
      change(set_attribute(:status_updated_at, &DateTime.utc_now/0, set_when_nil?: false))

      change(
        after_action(fn changeset, subject, _context ->
          AuditFact.record(%{
            installation_id: subject.installation_id,
            subject_id: subject.id,
            fact_kind: :subject_ingested,
            actor_ref: Ash.Changeset.get_argument(changeset, :actor_ref),
            trace_id: Ash.Changeset.get_argument(changeset, :trace_id),
            causation_id: Ash.Changeset.get_argument(changeset, :causation_id),
            payload: %{
              subject_kind: subject.subject_kind,
              lifecycle_state: subject.lifecycle_state,
              status: subject.status,
              source_ref: subject.source_ref
            },
            occurred_at: DateTime.utc_now()
          })
          |> case do
            {:ok, _fact} -> {:ok, subject}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    read :by_installation_source_ref do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:source_ref, :string, allow_nil?: false)
      get?(true)
      filter(expr(installation_id == ^arg(:installation_id) and source_ref == ^arg(:source_ref)))
    end

    update :advance_lifecycle do
      accept([])
      require_atomic?(false)

      argument(:lifecycle_state, :string, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:lifecycle_state, arg(:lifecycle_state)))

      change(
        after_action(fn changeset, subject, _context ->
          AuditFact.record(%{
            installation_id: subject.installation_id,
            subject_id: subject.id,
            fact_kind: :lifecycle_advanced,
            actor_ref: Ash.Changeset.get_argument(changeset, :actor_ref),
            trace_id: Ash.Changeset.get_argument(changeset, :trace_id),
            causation_id: Ash.Changeset.get_argument(changeset, :causation_id),
            payload: %{
              lifecycle_state: subject.lifecycle_state
            },
            occurred_at: DateTime.utc_now()
          })
          |> case do
            {:ok, _fact} -> {:ok, subject}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :block do
      accept([])
      require_atomic?(false)

      argument(:block_reason, :string, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:block_reason, arg(:block_reason)))
      change(set_attribute(:blocked_at, &DateTime.utc_now/0))

      change(
        after_action(fn changeset, subject, _context ->
          AuditFact.record(%{
            installation_id: subject.installation_id,
            subject_id: subject.id,
            fact_kind: :subject_blocked,
            actor_ref: Ash.Changeset.get_argument(changeset, :actor_ref),
            trace_id: Ash.Changeset.get_argument(changeset, :trace_id),
            causation_id: Ash.Changeset.get_argument(changeset, :causation_id),
            payload: %{
              lifecycle_state: subject.lifecycle_state,
              block_reason: subject.block_reason
            },
            occurred_at: DateTime.utc_now()
          })
          |> case do
            {:ok, _fact} -> {:ok, subject}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :unblock do
      accept([])
      require_atomic?(false)

      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))

      change(fn changeset, _context ->
        changeset
        |> Ash.Changeset.change_attribute(:block_reason, nil)
        |> Ash.Changeset.change_attribute(:blocked_at, nil)
      end)

      change(
        after_action(fn changeset, subject, _context ->
          AuditFact.record(%{
            installation_id: subject.installation_id,
            subject_id: subject.id,
            fact_kind: :subject_unblocked,
            actor_ref: Ash.Changeset.get_argument(changeset, :actor_ref),
            trace_id: Ash.Changeset.get_argument(changeset, :trace_id),
            causation_id: Ash.Changeset.get_argument(changeset, :causation_id),
            payload: %{
              lifecycle_state: subject.lifecycle_state
            },
            occurred_at: DateTime.utc_now()
          })
          |> case do
            {:ok, _fact} -> {:ok, subject}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :pause do
      accept([])
      require_atomic?(false)

      argument(:reason, :string)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "paused"))
      change(set_attribute(:status_reason, arg(:reason)))
      change(set_attribute(:status_updated_at, &DateTime.utc_now/0))

      change(
        after_action(fn changeset, subject, _context ->
          record_status_audit(changeset, subject, :subject_paused)
        end)
      )
    end

    update :resume do
      accept([])
      require_atomic?(false)

      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "active"))
      change(set_attribute(:status_reason, nil))
      change(set_attribute(:status_updated_at, &DateTime.utc_now/0))

      change(
        after_action(fn changeset, subject, _context ->
          record_status_audit(changeset, subject, :subject_resumed)
        end)
      )
    end

    update :cancel do
      accept([])
      require_atomic?(false)

      argument(:reason, :string)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "cancelled"))
      change(set_attribute(:status_reason, arg(:reason)))
      change(set_attribute(:status_updated_at, &DateTime.utc_now/0))
      change(set_attribute(:terminal_at, &DateTime.utc_now/0, set_when_nil?: false))

      change(
        after_action(fn changeset, subject, _context ->
          record_status_audit(changeset, subject, :subject_cancelled)
        end)
      )
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :installation_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :source_ref, :string do
      public?(true)
    end

    attribute :subject_kind, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :lifecycle_state, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :status, :string do
      allow_nil?(false)
      default("active")
      constraints(match: ~r/^(active|paused|cancelled)$/)
      public?(true)
    end

    attribute :status_reason, :string do
      public?(true)
    end

    attribute :status_updated_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :terminal_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :title, :string do
      public?(true)
    end

    attribute :description, :string do
      public?(true)
    end

    attribute :schema_ref, :string do
      public?(true)
    end

    attribute :schema_version, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
    end

    attribute :payload, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :opened_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :blocked_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :block_reason, :string do
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
    identity(:unique_installation_source_ref, [:installation_id, :source_ref])
  end

  defp record_status_audit(changeset, subject, fact_kind) do
    AuditFact.record(%{
      installation_id: subject.installation_id,
      subject_id: subject.id,
      fact_kind: fact_kind,
      actor_ref: Ash.Changeset.get_argument(changeset, :actor_ref),
      trace_id: Ash.Changeset.get_argument(changeset, :trace_id),
      causation_id: Ash.Changeset.get_argument(changeset, :causation_id),
      payload: %{
        lifecycle_state: subject.lifecycle_state,
        status: subject.status,
        status_reason: subject.status_reason,
        terminal_at: subject.terminal_at
      },
      occurred_at: DateTime.utc_now()
    })
    |> case do
      {:ok, _fact} -> {:ok, subject}
      {:error, error} -> {:error, error}
    end
  end
end
