# Mezzanine Workflow Runtime

Temporal worker and post-commit workflow dispatch boundary for Mezzanine.

## Production composition

The normal NSHKR release composes:

- `Mezzanine.Repo`
- `Mezzanine.WorkflowRuntime.Store.Postgres`
- `Mezzanine.WorkflowRuntime.RunOutboxDispatcher`
- `Mezzanine.WorkflowRuntime.TemporalSupervisor`
- the `Mezzanine.Workflows.NshkrAgentRun` worker on
  `nshkr.mezzanine.agent-run.v1`

`Mezzanine.WorkflowRuntime.Store.accept_run/2` commits run, first-turn, event,
projection, cursor, and workflow-outbox truth before any Temporal call. The
dispatcher claims committed rows with a lease, starts the exact source-owned
workflow module through `TemporalexAdapter`, then persists acknowledgement or
ambiguity before moving on.

The production child list can be built with
`Mezzanine.WorkflowRuntime.Application.production_child_specs/1`. The caller
must pass explicit `:temporal` and `:outbox_dispatcher` option sets. Unknown
Temporal bases/task queues and non-Postgres store selections fail closed.

Local Temporal substrate control remains repository-owned: use `just dev-up`,
`just dev-status`, `just dev-logs`, and `just temporal-ui` from the Mezzanine
root.

See `docs/persistence.md` for schema and preflight details.
