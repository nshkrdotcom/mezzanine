defmodule Mezzanine.Decisions.Repo.Migrations.AddExpiryJobIdToDecisionRecords do
  use Ecto.Migration

  def change do
    alter table(:decision_records) do
      add :expiry_job_id, :bigint
    end

    create index(:decision_records, [:expiry_job_id])
  end
end
