defmodule Mezzanine.OpsDomain.Repo.Migrations.Phase5ReviewUnitRowVersion do
  use Ecto.Migration

  def change do
    alter table(:review_units) do
      add(:row_version, :integer, null: false, default: 1)
    end
  end
end
