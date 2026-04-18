alias Ecto.Adapters.SQL
alias Mezzanine.Archival.Repo, as: ArchivalRepo

if is_nil(Process.whereis(ArchivalRepo)) do
  {:ok, _pid} = ArchivalRepo.start_link()
end

for statement <- [
      "DROP SCHEMA IF EXISTS public CASCADE",
      "CREATE SCHEMA public",
      "GRANT ALL ON SCHEMA public TO public",
      "CREATE EXTENSION IF NOT EXISTS pgcrypto"
    ] do
  SQL.query!(ArchivalRepo, statement, [])
end

for {task, args} <- [
      {"ecto.migrate",
       [
         "-r",
         "Mezzanine.ConfigRegistry.Repo",
         "--migrations-path",
         "../config_registry/priv/repo/migrations"
       ]},
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
         "../barriers/priv/repo/migrations"
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

for repo <- [Mezzanine.ConfigRegistry.Repo] do
  if is_nil(Process.whereis(repo)) do
    {:ok, _pid} = repo.start_link()
  end
end

ExUnit.start()
