ExUnit.configure(exclude: [live_temporal: true])
ExUnit.start()

{:ok, _pid} = Mezzanine.Repo.start_link()

Ecto.Migrator.run(
  Mezzanine.Repo,
  Application.app_dir(:mezzanine_core, "priv/repo/migrations"),
  :up,
  all: true
)
