defmodule Mezzanine.Audit.Repo.Migrations.AddM7AOrderingToMemoryProofTokens do
  use Ecto.Migration

  def change do
    alter table(:memory_proof_tokens) do
      add(:proof_hash_version, :text, null: false, default: "m6.v1")
      add(:source_node_ref, :text)
      add(:commit_lsn, :text)
      add(:commit_hlc, :map)
    end

    create constraint(:memory_proof_tokens, :memory_proof_tokens_hash_version_check,
             check: "proof_hash_version IN ('m6.v1', 'm7a.v1')"
           )

    create constraint(:memory_proof_tokens, :memory_proof_tokens_version_field_set_check,
             check: """
             (
               proof_hash_version = 'm6.v1'
               AND source_node_ref IS NULL
               AND commit_lsn IS NULL
               AND commit_hlc IS NULL
             )
             OR
             (
               proof_hash_version = 'm7a.v1'
               AND NULLIF(BTRIM(source_node_ref), '') IS NOT NULL
               AND NULLIF(BTRIM(commit_lsn), '') IS NOT NULL
               AND commit_hlc IS NOT NULL
             )
             """
           )

    create unique_index(
             :memory_proof_tokens,
             [:trace_id, :tenant_ref, :epoch_used, :user_ref, :agent_ref],
             name: :memory_proof_tokens_recall_idempotency_idx,
             where: "kind = 'recall'"
           )
  end
end
