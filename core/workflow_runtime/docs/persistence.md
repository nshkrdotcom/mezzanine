# Mezzanine Workflow Runtime Persistence

The production store is `Mezzanine.WorkflowRuntime.Store.Postgres`; there is no
production memory default or request-level backend selection.

The store uses `Mezzanine.OpsDomain.Repo` and verifies migration
`20260720111500` through `Store.preflight/1`. It exposes the exact operations
required by the first Synapse run journey:

- atomic canonical work/run-lineage and first-turn acceptance;
- idempotent command replay and hash-conflict rejection;
- durable projection and cursor readback;
- ordered event listing;
- leased workflow-outbox claiming and terminal outcome updates.

`RunOutboxDispatcher` is post-commit only. A Temporal start is impossible until
a pending outbox row is visible and atomically claimed. Successful or duplicate
starts become `acknowledged`; an uncertain response becomes `ambiguous` with a
redacted error ref.

The exact initial worker configuration is:

```elixir
temporal: [
  enabled?: true,
  namespace: "nshkr-production",
  task_queues: ["nshkr.mezzanine.agent-run.v1"],
  instance_base: Mezzanine.WorkflowRuntime.Temporal
]
```

Test-only deterministic stores may be enabled only by test configuration. They
cannot be selected by a normal release.
