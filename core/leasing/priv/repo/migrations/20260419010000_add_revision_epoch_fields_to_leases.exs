defmodule Mezzanine.Leasing.Repo.Migrations.AddRevisionEpochFieldsToLeases do
  use Ecto.Migration

  def change do
    alter table(:read_leases) do
      add(:installation_revision, :bigint, null: false)
      add(:activation_epoch, :bigint, null: false)
      add(:lease_epoch, :bigint, null: false)
    end

    alter table(:stream_attach_leases) do
      add(:installation_revision, :bigint, null: false)
      add(:activation_epoch, :bigint, null: false)
      add(:lease_epoch, :bigint, null: false)
    end

    alter table(:lease_invalidations) do
      add(:installation_revision, :bigint)
      add(:activation_epoch, :bigint)
      add(:lease_epoch, :bigint)
      add(:revocation_ref, :text, null: false)
      add(:cache_invalidation_ref, :text, null: false)
    end

    create(index(:read_leases, [:installation_revision, :activation_epoch, :lease_epoch]))

    create(
      index(:stream_attach_leases, [
        :installation_revision,
        :activation_epoch,
        :lease_epoch
      ])
    )
  end
end
