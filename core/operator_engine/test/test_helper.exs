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
         "Mezzanine.Execution.Repo",
         "--migrations-path",
         "../execution_engine/priv/repo/migrations"
       ]},
      {"ecto.migrate",
       [
         "-r",
         "Mezzanine.Execution.Repo",
         "--migrations-path",
         "../leasing/priv/repo/migrations"
       ]}
    ] do
  Mix.Task.reenable(task)
  Mix.Task.run(task, args)
end

ExUnit.start()
