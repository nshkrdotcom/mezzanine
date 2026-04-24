defmodule Mezzanine.Audit.Repo.Migrations.CreateMemoryProofTokens do
  use Ecto.Migration

  def change do
    create table(:memory_proof_tokens, primary_key: false) do
      add(:id, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:proof_id, :text, null: false)
      add(:kind, :text, null: false)
      add(:tenant_ref, :text, null: false)
      add(:installation_id, :text)
      add(:subject_id, :text)
      add(:execution_id, :text)
      add(:user_ref, :text)
      add(:agent_ref, :text)
      add(:t_event, :utc_datetime_usec, null: false)
      add(:epoch_used, :bigint, null: false)
      add(:policy_refs, {:array, :map}, null: false, default: [])
      add(:fragment_ids, {:array, :text}, null: false, default: [])
      add(:transform_hashes, {:array, :text}, null: false, default: [])
      add(:access_projection_hashes, {:array, :text}, null: false, default: [])
      add(:proof_hash, :text, null: false)
      add(:trace_id, :text, null: false)
      add(:parent_fragment_id, :text)
      add(:child_fragment_id, :text)
      add(:evidence_refs, {:array, :map}, null: false, default: [])
      add(:governance_decision_ref, :map)
      add(:metadata, :map, null: false, default: %{})

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:memory_proof_tokens, [:proof_id])
    create index(:memory_proof_tokens, [:tenant_ref, :trace_id, :t_event])
    create index(:memory_proof_tokens, [:tenant_ref, :proof_hash])
  end
end
