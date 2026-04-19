# MezzanineLifecycleEngine

Durable lifecycle coordination for the Mezzanine rebuild.

This package now owns the first real Stage `9.1` lifecycle slice:

- explicit `{:execution_requested, recipe_ref}` transition handling
- same-database transaction ownership for subject mutation plus durable dispatch
  enqueue
- installation-bound compiled-pack loading from the config registry
- trace-linked execution creation, lineage seeding, and audit facts for queued
  work
- receipt-driven lifecycle re-entry for accepted executions
- typed outcome mapping through
  `{:execution_completed, recipe_ref}` and
  `{:execution_failed, recipe_ref[, failure_kind]}`
- idempotent reconcile-to-receipt recovery for executions stranded in
  `:awaiting_receipt`
- installation revision gating before governed execution requests are queued;
  stale callers receive attempted/current revision diagnostics and no lower
  submission
- duplicate-safe `LifecycleContinuationWorker` processing for transient retry,
  deterministic dead-letter, and operator-forced retry/waive flows

Primary modules:

- `Mezzanine.LifecycleEvaluator`
- `Mezzanine.ExecutionReceiptWorker`
- `Mezzanine.ExecutionReconcileWorker`
- `Mezzanine.LifecycleContinuationWorker`
