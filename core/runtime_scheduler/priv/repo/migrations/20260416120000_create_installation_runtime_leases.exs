defmodule Mezzanine.RuntimeScheduler.Repo.Migrations.CreateInstallationRuntimeLeases do
  use Ecto.Migration

  def change do
    create table(:installation_runtime_leases, primary_key: false) do
      add :installation_id, :text, primary_key: true
      add :holder, :text, null: false
      add :lease_id, :text, null: false
      add :epoch, :bigint, null: false
      add :compiled_pack_revision, :bigint, null: false
      add :expires_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:installation_runtime_leases, [:installation_id])
  end
end
