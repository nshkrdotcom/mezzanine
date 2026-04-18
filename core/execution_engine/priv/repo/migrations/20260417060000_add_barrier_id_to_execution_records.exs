defmodule Mezzanine.Execution.Repo.Migrations.AddBarrierIdToExecutionRecords do
  use Ecto.Migration

  def change do
    alter table(:execution_records) do
      add :barrier_id, :uuid
    end

    create index(:execution_records, [:barrier_id])
  end
end
