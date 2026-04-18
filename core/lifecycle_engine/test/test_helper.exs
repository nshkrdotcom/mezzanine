for {task, args} <- [
      {"ecto.create", ["--quiet"]},
      {"ecto.migrate",
       ["-r", "Mezzanine.Audit.Repo", "--migrations-path", "../audit_engine/priv/repo/migrations"]},
      {"ecto.migrate",
       [
         "-r",
         "Mezzanine.Objects.Repo",
         "--migrations-path",
         "../object_engine/priv/repo/migrations"
       ]},
      {"ecto.migrate",
       [
         "-r",
         "Mezzanine.ConfigRegistry.Repo",
         "--migrations-path",
         "../config_registry/priv/repo/migrations"
       ]},
      {"ecto.migrate",
       [
         "-r",
         "Mezzanine.Execution.Repo",
         "--migrations-path",
         "../barriers/priv/repo/migrations"
       ]},
      {"ecto.migrate",
       [
         "-r",
         "Mezzanine.Execution.Repo",
         "--migrations-path",
         "../execution_engine/priv/repo/migrations"
       ]}
    ] do
  Mix.Task.reenable(task)
  Mix.Task.run(task, args)
end

ExUnit.start()

Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.Execution.Repo, :manual)
Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.Objects.Repo, :manual)
Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.Audit.Repo, :manual)
Ecto.Adapters.SQL.Sandbox.mode(Mezzanine.ConfigRegistry.Repo, :manual)
