defmodule Mezzanine.Execution.Repo.Migrations.AddReconcileWaveTracking do
  use Ecto.Migration

  def change do
    alter table(:execution_records) do
      add :last_reconcile_wave_id, :text
    end
  end
end
