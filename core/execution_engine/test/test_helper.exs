ExUnit.start()

Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.Execution.Repo, :manual)
Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.Objects.Repo, :manual)
Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.Audit.Repo, :manual)
