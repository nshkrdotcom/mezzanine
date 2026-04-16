defmodule Mezzanine.EvidenceLedger.EvidenceRecord do
  @moduledoc """
  Durable evidence ledger row for substrate-owned collection and verification truth.
  """

  use Ash.Resource,
    domain: Mezzanine.EvidenceLedger,
    data_layer: AshPostgres.DataLayer

  alias Mezzanine.Audit.AuditFact

  @complete_statuses ["collected", "verified"]

  postgres do
    table("evidence_records")
    repo(Mezzanine.EvidenceLedger.Repo)

    custom_indexes do
      index([:installation_id, :trace_id])
      index([:causation_id])
      index([:subject_id, :status])
      index([:execution_id, :status])
      index([:installation_id, :subject_id, :execution_id, :evidence_kind], unique: true)
    end
  end

  code_interface do
    define(:collect, action: :collect)
    define(:verify, action: :verify)
    define(:mark_failed, action: :mark_failed)
    define(:for_subject, action: :for_subject, args: [:subject_id])

    define(:for_subject_execution,
      action: :for_subject_execution,
      args: [:subject_id, :execution_id]
    )

    define(:by_subject_execution_kind,
      action: :by_subject_execution_kind,
      args: [:subject_id, :execution_id, :evidence_kind]
    )
  end

  actions do
    defaults([:read])

    create :collect do
      accept([
        :installation_id,
        :subject_id,
        :execution_id,
        :evidence_kind,
        :collector_ref,
        :content_ref,
        :status,
        :metadata
      ])

      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))

      change(fn changeset, _context ->
        set_collection_timestamps(changeset, Ash.Changeset.get_attribute(changeset, :status))
      end)

      change(
        after_action(fn changeset, evidence, _context ->
          record_audit_fact(
            evidence,
            Ash.Changeset.get_argument(changeset, :actor_ref),
            :evidence_collected,
            %{
              evidence_kind: evidence.evidence_kind,
              status: evidence.status,
              collector_ref: evidence.collector_ref,
              content_ref: evidence.content_ref
            }
          )
          |> case do
            {:ok, _fact} -> {:ok, evidence}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :verify do
      accept([])
      require_atomic?(false)

      argument(:content_ref, :string)
      argument(:metadata, :map, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "verified"))
      change(set_attribute(:content_ref, arg(:content_ref)))
      change(set_attribute(:metadata, arg(:metadata)))
      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))
      change(set_attribute(:verified_at, &DateTime.utc_now/0))

      change(fn changeset, _context ->
        if is_nil(Ash.Changeset.get_attribute(changeset, :collected_at)) do
          Ash.Changeset.change_attribute(changeset, :collected_at, DateTime.utc_now())
        else
          changeset
        end
      end)

      change(
        after_action(fn changeset, evidence, _context ->
          record_audit_fact(
            evidence,
            Ash.Changeset.get_argument(changeset, :actor_ref),
            :evidence_verified,
            %{
              evidence_kind: evidence.evidence_kind,
              status: evidence.status,
              content_ref: evidence.content_ref
            }
          )
          |> case do
            {:ok, _fact} -> {:ok, evidence}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    update :mark_failed do
      accept([])
      require_atomic?(false)

      argument(:metadata, :map, allow_nil?: false)
      argument(:trace_id, :string, allow_nil?: false)
      argument(:causation_id, :string, allow_nil?: false)
      argument(:actor_ref, :map, allow_nil?: false)

      change(optimistic_lock(:row_version))
      change(set_attribute(:status, "failed"))
      change(set_attribute(:metadata, arg(:metadata)))
      change(set_attribute(:trace_id, arg(:trace_id)))
      change(set_attribute(:causation_id, arg(:causation_id)))

      change(
        after_action(fn changeset, evidence, _context ->
          record_audit_fact(
            evidence,
            Ash.Changeset.get_argument(changeset, :actor_ref),
            :evidence_collected,
            %{
              evidence_kind: evidence.evidence_kind,
              status: evidence.status,
              content_ref: evidence.content_ref,
              metadata: evidence.metadata
            }
          )
          |> case do
            {:ok, _fact} -> {:ok, evidence}
            {:error, error} -> {:error, error}
          end
        end)
      )
    end

    read :for_subject do
      argument(:subject_id, :uuid, allow_nil?: false)
      filter(expr(subject_id == ^arg(:subject_id)))
      prepare(build(sort: [updated_at: :asc, inserted_at: :asc]))
    end

    read :for_subject_execution do
      argument(:subject_id, :uuid, allow_nil?: false)
      argument(:execution_id, :uuid, allow_nil?: false)
      filter(expr(subject_id == ^arg(:subject_id) and execution_id == ^arg(:execution_id)))
      prepare(build(sort: [updated_at: :asc, inserted_at: :asc]))
    end

    read :by_subject_execution_kind do
      argument(:subject_id, :uuid, allow_nil?: false)
      argument(:execution_id, :uuid, allow_nil?: false)
      argument(:evidence_kind, :string, allow_nil?: false)

      get?(true)

      filter(
        expr(
          subject_id == ^arg(:subject_id) and execution_id == ^arg(:execution_id) and
            evidence_kind == ^arg(:evidence_kind)
        )
      )
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

    attribute :evidence_kind, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :collector_ref, :string do
      public?(true)
    end

    attribute :content_ref, :string do
      public?(true)
    end

    attribute :status, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :metadata, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :collected_at, :utc_datetime_usec do
      public?(true)
    end

    attribute :verified_at, :utc_datetime_usec do
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

  @spec verify_or_update(Ecto.UUID.t(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, struct()} | {:error, term()}
  def verify_or_update(subject_id, execution_id, evidence_kind, attrs) do
    case by_subject_execution_kind(subject_id, execution_id, evidence_kind) do
      {:ok, nil} ->
        collect(
          Map.merge(attrs, %{
            subject_id: subject_id,
            execution_id: execution_id,
            evidence_kind: evidence_kind
          })
        )

      {:ok, record} ->
        verify(record, attrs)

      {:error, error} ->
        {:error, error}
    end
  end

  @spec complete_status?(String.t()) :: boolean()
  def complete_status?(status), do: status in @complete_statuses

  defp set_collection_timestamps(changeset, "collected") do
    Ash.Changeset.change_attribute(changeset, :collected_at, DateTime.utc_now())
  end

  defp set_collection_timestamps(changeset, "verified") do
    changeset
    |> Ash.Changeset.change_attribute(:collected_at, DateTime.utc_now())
    |> Ash.Changeset.change_attribute(:verified_at, DateTime.utc_now())
  end

  defp set_collection_timestamps(changeset, _status), do: changeset

  defp record_audit_fact(evidence, actor_ref, fact_kind, payload) do
    AuditFact.record(%{
      installation_id: evidence.installation_id,
      subject_id: evidence.subject_id,
      execution_id: evidence.execution_id,
      evidence_id: evidence.id,
      trace_id: evidence.trace_id,
      causation_id: evidence.causation_id,
      fact_kind: fact_kind,
      actor_ref: actor_ref,
      payload: payload,
      occurred_at: DateTime.utc_now()
    })
  end
end
