defmodule Mezzanine.Objects.SubjectRecord do
  @moduledoc """
  Durable substrate-owned subject ledger row with canonical lifecycle ownership.
  """

  use Ash.Resource,
    domain: Mezzanine.Objects,
    data_layer: AshPostgres.DataLayer

  alias Mezzanine.Audit.AuditAppend
  alias Mezzanine.Objects.SubjectPayloadSchema

  postgres do
    table("subject_records")
    repo(Mezzanine.Objects.Repo)

    custom_indexes do
      index([:installation_id, :source_ref], unique: true)
      index([:installation_id, :source_binding_id, :provider_external_ref])
      index([:installation_id, :lifecycle_state])
      index([:installation_id, :subject_kind])
      index([:installation_id, :source_state])
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
        :source_event_id,
        :source_binding_id,
        :provider,
        :provider_external_ref,
        :provider_revision,
        :source_state,
        :state_mapping,
        :blocker_refs,
        :labels,
        :priority,
        :branch_ref,
        :source_url,
        :workpad_ref,
        :progress_ref,
        :source_routing,
        :lifecycle_version,
        :payload_schema_revision,
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
      change(&bind_payload_schema/2)

      change(
        after_action(fn changeset, subject, _context ->
          append_audit_fact(changeset, subject, :subject_ingested, %{
            subject_kind: subject.subject_kind,
            lifecycle_state: subject.lifecycle_state,
            status: subject.status,
            source_ref: subject.source_ref,
            source_event_id: subject.source_event_id,
            source_binding_id: subject.source_binding_id,
            provider: subject.provider,
            provider_external_ref: subject.provider_external_ref,
            provider_revision: subject.provider_revision,
            source_state: subject.source_state,
            workpad_ref: subject.workpad_ref,
            progress_ref: subject.progress_ref,
            schema_ref: subject.schema_ref,
            schema_version: subject.schema_version,
            schema_hash:
              SubjectPayloadSchema.schema_hash!(
                subject.subject_kind,
                subject.schema_ref,
                subject.schema_version
              )
          })
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
          append_audit_fact(changeset, subject, :lifecycle_advanced, %{
            lifecycle_state: subject.lifecycle_state
          })
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
          append_audit_fact(changeset, subject, :subject_blocked, %{
            lifecycle_state: subject.lifecycle_state,
            block_reason: subject.block_reason
          })
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
          append_audit_fact(changeset, subject, :subject_unblocked, %{
            lifecycle_state: subject.lifecycle_state
          })
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
      argument(:operator_context, :map)

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
      argument(:operator_context, :map)

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
      argument(:operator_context, :map)

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

    attribute :source_event_id, :string do
      public?(true)
    end

    attribute :source_binding_id, :string do
      public?(true)
    end

    attribute :provider, :string do
      public?(true)
    end

    attribute :provider_external_ref, :string do
      public?(true)
    end

    attribute :provider_revision, :string do
      public?(true)
    end

    attribute :source_state, :string do
      public?(true)
    end

    attribute :state_mapping, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :blocker_refs, {:array, :map} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :labels, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :priority, :integer do
      public?(true)
    end

    attribute :branch_ref, :string do
      public?(true)
    end

    attribute :source_url, :string do
      public?(true)
    end

    attribute :workpad_ref, :string do
      public?(true)
    end

    attribute :progress_ref, :string do
      public?(true)
    end

    attribute :source_routing, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :lifecycle_version, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
    end

    attribute :payload_schema_revision, :string do
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

  defp bind_payload_schema(changeset, _context) do
    attrs = %{
      subject_kind: Ash.Changeset.get_attribute(changeset, :subject_kind),
      schema_ref: Ash.Changeset.get_attribute(changeset, :schema_ref),
      schema_version: Ash.Changeset.get_attribute(changeset, :schema_version),
      payload: Ash.Changeset.get_attribute(changeset, :payload)
    }

    case SubjectPayloadSchema.validate_ingest(attrs) do
      {:ok, %{payload: payload}} ->
        Ash.Changeset.change_attribute(changeset, :payload, payload)

      {:error, reason} ->
        Ash.Changeset.add_error(
          changeset,
          field: :payload,
          message: "subject payload must match a source-owned schema: #{inspect(reason)}"
        )
    end
  end

  defp record_status_audit(changeset, subject, fact_kind) do
    payload = %{
      lifecycle_state: subject.lifecycle_state,
      status: subject.status,
      status_reason: subject.status_reason,
      terminal_at: subject.terminal_at
    }

    payload =
      case Ash.Changeset.get_argument(changeset, :operator_context) do
        context when is_map(context) -> Map.merge(payload, context)
        _other -> payload
      end

    append_audit_fact(changeset, subject, fact_kind, payload)
  end

  defp append_audit_fact(changeset, subject, fact_kind, payload) do
    AuditAppend.append_fact(
      %{
        installation_id: subject.installation_id,
        subject_id: subject.id,
        fact_kind: fact_kind,
        actor_ref: Ash.Changeset.get_argument(changeset, :actor_ref),
        trace_id: Ash.Changeset.get_argument(changeset, :trace_id),
        causation_id: Ash.Changeset.get_argument(changeset, :causation_id),
        payload: payload,
        occurred_at: DateTime.utc_now()
      },
      []
    )
    |> case do
      {:ok, _fact} -> {:ok, subject}
      {:error, error} -> {:error, error}
    end
  end
end
