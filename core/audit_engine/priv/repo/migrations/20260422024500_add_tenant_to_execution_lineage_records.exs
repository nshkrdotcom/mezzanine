defmodule Mezzanine.Audit.Repo.Migrations.AddTenantToExecutionLineageRecords do
  use Ecto.Migration

  def change do
    alter table(:execution_lineage_records) do
      add :tenant_id, :text, null: false, default: ""
    end

    create index(:execution_lineage_records, [:tenant_id, :installation_id, :trace_id])
  end
end
