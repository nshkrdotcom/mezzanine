defmodule Mezzanine.Leasing.Repo.Migrations.AddRevisionEpochFieldsToLeases do
  use Ecto.Migration

  def change do
    execute(
      """
      ALTER TABLE read_leases
        ADD COLUMN IF NOT EXISTS installation_revision bigint NOT NULL,
        ADD COLUMN IF NOT EXISTS activation_epoch bigint NOT NULL,
        ADD COLUMN IF NOT EXISTS lease_epoch bigint NOT NULL
      """,
      """
      ALTER TABLE read_leases
        DROP COLUMN IF EXISTS lease_epoch,
        DROP COLUMN IF EXISTS activation_epoch,
        DROP COLUMN IF EXISTS installation_revision
      """
    )

    execute(
      """
      ALTER TABLE stream_attach_leases
        ADD COLUMN IF NOT EXISTS installation_revision bigint NOT NULL,
        ADD COLUMN IF NOT EXISTS activation_epoch bigint NOT NULL,
        ADD COLUMN IF NOT EXISTS lease_epoch bigint NOT NULL
      """,
      """
      ALTER TABLE stream_attach_leases
        DROP COLUMN IF EXISTS lease_epoch,
        DROP COLUMN IF EXISTS activation_epoch,
        DROP COLUMN IF EXISTS installation_revision
      """
    )

    execute(
      """
      ALTER TABLE lease_invalidations
        ADD COLUMN IF NOT EXISTS installation_revision bigint,
        ADD COLUMN IF NOT EXISTS activation_epoch bigint,
        ADD COLUMN IF NOT EXISTS lease_epoch bigint,
        ADD COLUMN IF NOT EXISTS revocation_ref text NOT NULL,
        ADD COLUMN IF NOT EXISTS cache_invalidation_ref text NOT NULL
      """,
      """
      ALTER TABLE lease_invalidations
        DROP COLUMN IF EXISTS cache_invalidation_ref,
        DROP COLUMN IF EXISTS revocation_ref,
        DROP COLUMN IF EXISTS lease_epoch,
        DROP COLUMN IF EXISTS activation_epoch,
        DROP COLUMN IF EXISTS installation_revision
      """
    )

    create_if_not_exists(
      index(:read_leases, [:installation_revision, :activation_epoch, :lease_epoch])
    )

    create_if_not_exists(
      index(:stream_attach_leases, [
        :installation_revision,
        :activation_epoch,
        :lease_epoch
      ])
    )
  end
end
