defmodule Mezzanine.Execution.DispatchOutboxEntry do
  @moduledoc """
  Durable single-row dispatch outbox owned solely by the substrate.
  """

  use Ash.Resource,
    domain: Mezzanine.Execution,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("dispatch_outbox_entries")
    repo(Mezzanine.Execution.Repo)

    custom_indexes do
      index([:execution_id], unique: true)
      index([:status, :available_at])
      index([:installation_id, :submission_dedupe_key], unique: true)
    end
  end

  code_interface do
    define(:enqueue, action: :enqueue)
    define(:by_execution_id, action: :by_execution_id, args: [:execution_id])
    define(:mark_completed, action: :mark_completed)
    define(:mark_pending_retry, action: :mark_pending_retry)
    define(:mark_terminal, action: :mark_terminal)
  end

  actions do
    defaults([:read])

    create :enqueue do
      accept([
        :execution_id,
        :installation_id,
        :subject_id,
        :trace_id,
        :causation_id,
        :status,
        :dispatch_envelope,
        :submission_dedupe_key,
        :compiled_pack_revision,
        :binding_snapshot,
        :available_at
      ])
    end

    read :by_execution_id do
      argument(:execution_id, :uuid, allow_nil?: false)
      get?(true)
      filter(expr(execution_id == ^arg(:execution_id)))
    end

    update :mark_completed do
      accept([])
      require_atomic?(false)
      change(set_attribute(:status, :completed))
      change(set_attribute(:last_error_kind, nil))
      change(set_attribute(:last_error_payload, %{}))
    end

    update :mark_pending_retry do
      accept([])
      require_atomic?(false)
      argument(:available_at, :utc_datetime_usec, allow_nil?: false)
      argument(:last_error_kind, :string, allow_nil?: false)
      argument(:last_error_payload, :map, allow_nil?: false)

      change(set_attribute(:status, :pending_retry))
      change(set_attribute(:available_at, arg(:available_at)))
      change(set_attribute(:last_error_kind, arg(:last_error_kind)))
      change(set_attribute(:last_error_payload, arg(:last_error_payload)))
    end

    update :mark_terminal do
      accept([])
      require_atomic?(false)
      argument(:last_error_kind, :string, allow_nil?: false)
      argument(:last_error_payload, :map, allow_nil?: false)

      change(set_attribute(:status, :terminal))
      change(set_attribute(:last_error_kind, arg(:last_error_kind)))
      change(set_attribute(:last_error_payload, arg(:last_error_payload)))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :execution_id, :uuid do
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

    attribute :trace_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :causation_id, :string do
      public?(true)
    end

    attribute :status, :atom do
      allow_nil?(false)
      default(:pending)
      constraints(one_of: [:pending, :dispatching, :pending_retry, :completed, :terminal])
      public?(true)
    end

    attribute :dispatch_envelope, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :submission_dedupe_key, :string do
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

    attribute :available_at, :utc_datetime_usec do
      allow_nil?(false)
      public?(true)
    end

    attribute :last_error_kind, :string do
      public?(true)
    end

    attribute :last_error_payload, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_execution_outbox, [:execution_id])
    identity(:unique_submission_dedupe_key, [:installation_id, :submission_dedupe_key])
  end
end
