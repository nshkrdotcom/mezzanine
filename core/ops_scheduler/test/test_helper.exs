ExUnit.start()

{:ok, _} = Application.ensure_all_started(:mezzanine_ops_scheduler)

migrations_path = Path.expand("../../ops_domain/priv/repo/migrations", __DIR__)

{:ok, _, _} =
  Ecto.Migrator.with_repo(Mezzanine.OpsDomain.Repo, fn repo ->
    Ecto.Migrator.run(repo, migrations_path, :up, all: true, log: false)
  end)

Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.OpsDomain.Repo, :manual)
