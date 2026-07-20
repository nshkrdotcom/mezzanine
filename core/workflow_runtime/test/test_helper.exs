ExUnit.configure(exclude: [live_temporal: true])
ExUnit.start()

{:ok, _pid} = Mezzanine.OpsDomain.Repo.start_link()

Ecto.Migrator.run(
  Mezzanine.OpsDomain.Repo,
  Application.app_dir(:mezzanine_ops_domain, "priv/repo/migrations"),
  :up,
  all: true
)
