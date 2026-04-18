defmodule Mezzanine.Objects.Repo.Migrations.AddOperatorStatusToSubjectRecords do
  use Ecto.Migration

  def change do
    alter table(:subject_records) do
      add :status, :text, null: false, default: "active"
      add :status_reason, :text
      add :status_updated_at, :utc_datetime_usec
      add :terminal_at, :utc_datetime_usec
    end

    execute(
      """
      UPDATE subject_records
      SET status = 'active',
          status_updated_at = COALESCE(opened_at, inserted_at, NOW())
      WHERE status IS NULL
      """,
      """
      UPDATE subject_records
      SET status = 'active',
          status_updated_at = COALESCE(opened_at, inserted_at, NOW())
      WHERE status IS NULL
      """
    )

    create index(:subject_records, [:installation_id, :status])
  end
end
