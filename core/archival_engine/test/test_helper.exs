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
         "Mezzanine.Decisions.Repo",
         "--migrations-path",
         "../decision_engine/priv/repo/migrations"
       ]},
      {"ecto.migrate",
       [
         "-r",
         "Mezzanine.EvidenceLedger.Repo",
         "--migrations-path",
         "../evidence_engine/priv/repo/migrations"
       ]},
      {"ecto.migrate", []}
    ] do
  Mix.Task.reenable(task)
  Mix.Task.run(task, args)
end

ExUnit.start()
