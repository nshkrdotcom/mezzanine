defmodule Mezzanine.Execution.ObanMigrationTest do
  use ExUnit.Case, async: true

  @migrations_dir Path.expand("../../../priv/repo/migrations", __DIR__)
  @migration_path Path.join(@migrations_dir, "20260428114100_update_oban_to_v14.exs")

  test "execution repo migrates oban metadata to the packaged dependency requirement" do
    migration = File.read!(@migration_path)

    assert migration =~ "Oban.Migrations.up(version: 14)"
    assert migration =~ "Oban.Migrations.down(version: 12)"
  end
end
