defmodule Mezzanine.Decisions.Repo.Migrations.DropDecisionExpiryJobId do
  use Ecto.Migration

  def change do
    drop_if_exists(index(:decision_records, [:expiry_job_id]))

    alter table(:decision_records) do
      remove_if_exists(:expiry_job_id, :bigint)
    end
  end
end
