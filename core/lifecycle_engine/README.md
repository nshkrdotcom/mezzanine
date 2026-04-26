# MezzanineLifecycleEngine

Durable lifecycle coordination for the Mezzanine rebuild.

This package now owns the first real Stage `9.1` lifecycle slice:

- explicit `{:execution_requested, recipe_ref}` transition handling
- same-database transaction ownership for subject mutation, execution record
  creation, `workflow_start_outbox` insert, and the Oban dispatch job that
  wakes the WorkflowRuntime outbox worker
- ref-only workflow handoff metadata: deterministic workflow ID,
  idempotency key, command receipt ref, workflow input ref, authority packet
  ref, and permission decision ref; this package does not compile or call the
  Temporal runtime directly
- installation-bound compiled-pack loading from the config registry
- trace-linked execution creation, lineage seeding, and audit facts for queued
  work
- receipt-driven lifecycle re-entry owned by workflow receipt signals
- typed outcome mapping through
  `{:execution_completed, recipe_ref}` and
  `{:execution_failed, recipe_ref[, failure_kind]}`
- idempotent reconcile-to-receipt recovery owned by Temporal workflow
  activities for executions projected as canonical `:accepted_active`
- installation revision gating before governed execution requests are queued;
  stale callers receive attempted/current revision diagnostics and no lower
  submission
- old receipt, reconcile, join, and lifecycle continuation Oban saga workers
  are blocked by `Mezzanine.WorkflowRuntime.FinalTemporalCutover` source scans;
  no retired worker tombstone modules remain in runtime source

Primary modules:

- `Mezzanine.LifecycleEvaluator`
