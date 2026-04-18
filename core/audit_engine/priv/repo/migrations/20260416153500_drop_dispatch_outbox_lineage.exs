defmodule Mezzanine.Audit.Repo.Migrations.DropDispatchOutboxLineage do
  use Ecto.Migration

  def change do
    alter table(:execution_lineage_records) do
      remove :dispatch_outbox_entry_id
    end
  end
end
