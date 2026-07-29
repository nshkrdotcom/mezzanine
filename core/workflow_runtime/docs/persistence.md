# Mezzanine Workflow Runtime Persistence

The production store is `Mezzanine.WorkflowRuntime.Store.Postgres`; there is no
production memory default or request-level backend selection.

The store uses `Mezzanine.OpsDomain.Repo` and verifies migration
`20260728130000` through `Store.preflight/1`. It exposes the exact operations
required by the first Synapse run journey:

- atomic canonical work/run-lineage and first-turn acceptance;
- idempotent command replay and hash-conflict rejection;
- durable projection and cursor readback;
- ordered event listing;
- leased workflow-outbox claiming and terminal outcome updates;
- optimistic run-control state, idempotent commands, and append-only events;
- a fenced, durable Temporal signal outbox;
- deadline and restart reconciliation driven by database time.

`RunOutboxDispatcher` is post-commit only. A Temporal start is impossible until
a pending outbox row is visible and atomically claimed. Successful or duplicate
starts become `acknowledged`; an uncertain response becomes `ambiguous` with a
redacted error ref.

`RecoveryControl` never replays a provider effect. Unknown outcomes retain the
same attempt and external operation refs, enter `reconciling`, and resolve only
from provider status readback or `operator_required`. Startup claims increment
a monotonic fence so a stale reconciler cannot commit.

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
