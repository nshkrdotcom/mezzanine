defmodule Mezzanine.Execution.Repo.Migrations.AddDispatchSnapshotsAndFailureKind do
  use Ecto.Migration

  def change do
    alter table(:execution_records) do
      add :compiled_pack_revision, :integer, null: false, default: 1
      add :binding_snapshot, :map, null: false, default: %{}
      add :failure_kind, :text
    end

    alter table(:dispatch_outbox_entries) do
      add :compiled_pack_revision, :integer, null: false, default: 1
      add :binding_snapshot, :map, null: false, default: %{}
    end
  end
end
