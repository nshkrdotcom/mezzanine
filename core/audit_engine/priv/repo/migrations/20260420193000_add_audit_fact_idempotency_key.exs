defmodule Mezzanine.Audit.Repo.Migrations.AddAuditFactIdempotencyKey do
  use Ecto.Migration

  def change do
    alter table(:audit_facts) do
      add :idempotency_key, :text
    end

    create unique_index(:audit_facts, [:installation_id, :idempotency_key])
  end
end
