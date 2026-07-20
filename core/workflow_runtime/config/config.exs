import Config

config :mezzanine_core,
  run_store: Mezzanine.WorkflowRuntime.Store.Postgres,
  workflow_runtime_impl: Mezzanine.WorkflowRuntime.TemporalexAdapter

config :mezzanine_workflow_runtime, ecto_repos: [Mezzanine.OpsDomain.Repo]

config :mezzanine_workflow_runtime, :temporal,
  enabled?: false,
  address: "127.0.0.1:7233",
  namespace: "default",
  instance_base: Mezzanine.WorkflowRuntime.Temporal,
  max_concurrent_workflow_tasks: 5,
  max_concurrent_activity_tasks: 5,
  headers: []

config :mezzanine_workflow_runtime, :outbox_persistence,
  repo: Mezzanine.Execution.Repo,
  store: Mezzanine.WorkflowRuntime.OutboxPersistence.SQL

import_config "#{config_env()}.exs"
