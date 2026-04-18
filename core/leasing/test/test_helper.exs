ExUnit.start()

{:ok, _} = Mezzanine.Leasing.Repo.start_link()
Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.Leasing.Repo, :manual)
