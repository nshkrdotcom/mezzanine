defmodule Mezzanine.Execution.Repo.Migrations.AddTenantSupersessionAndCircuits do
  use Ecto.Migration

  def change do
    alter table(:execution_records) do
      add :tenant_id, :text, null: false, default: ""
      add :supersedes_execution_id, :uuid
      add :supersession_reason, :text
      add :supersession_depth, :integer, null: false, default: 0
    end

    create index(:execution_records, [:tenant_id, :installation_id])
    create index(:execution_records, [:supersedes_execution_id])

    create table(:lower_gateway_circuits, primary_key: false) do
      add :tenant_id, :text, null: false
      add :installation_id, :text, null: false
      add :state, :text, null: false
      add :error_count, :integer, null: false, default: 0
      add :window_started_at, :utc_datetime_usec
      add :opened_at, :utc_datetime_usec
      add :last_probe_at, :utc_datetime_usec
      add :generation, :integer, null: false, default: 1

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:lower_gateway_circuits, [:tenant_id, :installation_id],
             name: :lower_gateway_circuits_scope_index
           )
  end
end
