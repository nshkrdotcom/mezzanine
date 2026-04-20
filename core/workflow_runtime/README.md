# Mezzanine Workflow Runtime

Owns the Phase 4 Temporal runtime boundary for Mezzanine.

This package is the only Mezzanine package that compiles the direct
`temporalex` runtime dependency. Core substrate packages keep pure contract and
ledger ownership; workflow execution code stays isolated here so monorepo
quality checks do not compile the Temporal Rust/NIF bridge transitively through
every engine.

## Workflow Starter Outbox

`Mezzanine.WorkflowRuntime.WorkflowStarterOutbox` defines the Phase 4 starter
outbox contract used by the accepted command transaction. The same local
Postgres transaction must persist the accepted command receipt, the
`workflow_start_outbox` row, and the Oban `workflow_start_outbox` dispatch job.

`Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker` is a bounded Oban
dispatcher. It does not contain workflow business logic and never talks to
Temporal directly; it builds a compact start request and calls
`Mezzanine.WorkflowRuntime.start_workflow/1`, which is the only public
Mezzanine Temporal client facade.

The row and job args carry refs, hashes, deterministic workflow identity,
authority/decision refs, trace/idempotency scope, release-manifest ref, and
scalar routing metadata only. Raw payloads, Temporal SDK structs, Temporal
protobufs, NIF resources, task tokens, and raw history events stay outside the
public DTO boundary.

## Activity Side-Effect Idempotency

`Mezzanine.WorkflowRuntime.ActivitySideEffectIdempotency` defines the M29
activity boundary used by Temporalex BEAM workers. It registers the workflow
activity versions for Jido lower submission, Execution Plane side effects, and
Outer Brain semantic payload boundaries while keeping owner repos free of
Temporal SDK imports.

Lower, execution, attach, heartbeat, cancellation, and connector-effect
activities acquire authority evidence through `Mezzanine.ActivityLeaseBroker`.
The broker cache is process-local to the activity worker and never enters
workflow history; workflow history carries only compact refs, hashes, bounded
routing facts, validation state, and diagnostics refs.

## Workflow Fan-Out/Fan-In

`Mezzanine.WorkflowRuntime.WorkflowFanoutFanin` defines the M30
`Mezzanine.WorkflowFanoutFanin.v1` contract for `Mezzanine.Workflows.JoinBarrier`.
Fan-out uses child workflows when each branch owns an independent durable
lifecycle. Every branch carries tenant, resource, trace, parent workflow ref,
child workflow ref, idempotency scope, authority context, and release-manifest
ref before the parent accepts it.

Child completions enter the parent through the `child.completed`
`child-completed.v1` signal shape. The parent closes the join barrier exactly
once, suppresses duplicate completions by completion idempotency key, exposes a
raw-payload-free `fanout.branch_state` query projection, and emits child cancel
signals only for unfinished branches with the original authority context.

Phase 5 hardening keeps this as a parent-workflow contract and adds explicit
close policy evidence for existing fan-out/fan-in paths. Supported close
policies are `all_required`, `k_of_n`, `at_least_one`,
`best_effort_with_required`, and `fail_fast`. A close decision may be
`succeeded`, `partial_success`, or `failed`; duplicate and late child
completions after close emit evidence but cannot increment `close_count` or
change the close decision. Heterogeneous branch failures are reported by
branch ref, failure class, safe action, and compensation ref without storing
raw child payloads in workflow history or Postgres projections.
