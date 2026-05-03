defmodule Mezzanine.Archival.ArchivalManifest do
  @moduledoc """
  Durable archival manifest carrying one archived subject graph plus its
  cold-storage and hot-row lifecycle.
  """

  use Ash.Resource, domain: Mezzanine.Archival, data_layer: AshPostgres.DataLayer

  postgres do
    table("archival_manifests")
    repo(Mezzanine.Archival.Repo)

    custom_indexes do
      index([:manifest_ref], unique: true)
      index([:installation_id, :status, :due_at])
      index([:installation_id, :subject_id, :terminal_at])
    end
  end

  code_interface do
    define(:stage, action: :stage)
    define(:mark_verified, action: :mark_verified)
    define(:mark_archived, action: :mark_archived)
    define(:mark_failed, action: :mark_failed)
    define(:by_manifest_ref, action: :by_manifest_ref, args: [:manifest_ref])

    define(:for_subject,
      action: :for_subject,
      args: [:installation_id, :subject_id]
    )
  end

  actions do
    defaults([:read])

    create :stage do
      accept([
        :manifest_ref,
        :installation_id,
        :subject_id,
        :subject_state,
        :execution_states,
        :trace_ids,
        :execution_ids,
        :decision_ids,
        :evidence_ids,
        :audit_fact_ids,
        :projection_names,
        :terminal_at,
        :due_at,
        :retention_seconds,
        :storage_kind,
        :metadata
      ])

      upsert?(true)
      upsert_identity(:unique_manifest_ref)

      upsert_fields([
        :subject_state,
        :execution_states,
        :trace_ids,
        :execution_ids,
        :decision_ids,
        :evidence_ids,
        :audit_fact_ids,
        :projection_names,
        :terminal_at,
        :due_at,
        :retention_seconds,
        :storage_kind,
        :metadata
      ])

      change(set_attribute(:status, "staging"))
      change(set_attribute(:storage_uri, nil))
      change(set_attribute(:checksum, nil))
      change(set_attribute(:verified_at, nil))
      change(set_attribute(:archived_at, nil))
      change(set_attribute(:failure_reason, nil))
    end

    update :mark_verified do
      accept([])
      require_atomic?(false)

      argument(:storage_uri, :string, allow_nil?: false)
      argument(:checksum, :string, allow_nil?: false)
      argument(:verified_at, :utc_datetime_usec, allow_nil?: false)
      argument(:metadata, :map, default: %{})

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "verified"))
      change(set_attribute(:storage_uri, arg(:storage_uri)))
      change(set_attribute(:checksum, arg(:checksum)))
      change(set_attribute(:verified_at, arg(:verified_at)))
      change(set_attribute(:metadata, arg(:metadata)))
      change(set_attribute(:failure_reason, nil))
    end

    update :mark_archived do
      accept([])
      require_atomic?(false)

      argument(:archived_at, :utc_datetime_usec, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "archived"))
      change(set_attribute(:archived_at, arg(:archived_at)))
      change(set_attribute(:failure_reason, nil))
    end

    update :mark_failed do
      accept([])
      require_atomic?(false)

      argument(:reason, :string, allow_nil?: false)
      argument(:metadata, :map, default: %{})

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "failed"))
      change(set_attribute(:failure_reason, arg(:reason)))
      change(set_attribute(:metadata, arg(:metadata)))
    end

    read :by_manifest_ref do
      argument(:manifest_ref, :string, allow_nil?: false)
      get?(true)
      filter(expr(manifest_ref == ^arg(:manifest_ref)))
    end

    read :for_subject do
      argument(:installation_id, :string, allow_nil?: false)
      argument(:subject_id, :uuid, allow_nil?: false)

      filter(expr(installation_id == ^arg(:installation_id) and subject_id == ^arg(:subject_id)))
      prepare(build(sort: [terminal_at: :desc, inserted_at: :desc]))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :manifest_ref, :string do
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

    attribute :subject_state, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :execution_states, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :trace_ids, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :execution_ids, {:array, :uuid} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :decision_ids, {:array, :uuid} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :evidence_ids, {:array, :uuid} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :audit_fact_ids, {:array, :uuid} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :projection_names, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :terminal_at, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    attribute :due_at, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    attribute :retention_seconds, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :storage_kind, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :status, :string do
      allow_nil?(false)
      default("staging")
      public?(true)
    end

    attribute :storage_uri, :string do
      public?(true)
    end

    attribute :checksum, :string do
      public?(true)
    end

    attribute :verified_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :archived_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :failure_reason, :string do
      public?(true)
    end

    attribute :metadata, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :row_version, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_manifest_ref, [:manifest_ref])
  end
end
