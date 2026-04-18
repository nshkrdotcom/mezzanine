defmodule Mezzanine.Execution.Repo.Migrations.AddIntentSnapshotToExecutionRecords do
  use Ecto.Migration

  def change do
    alter table(:execution_records) do
      add(:intent_snapshot, :map, null: false, default: %{})
    end
  end
end
