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
The lifecycle engine produces only scalar refs, hashes, deterministic workflow
identity, and idempotency keys for this contract; it does not depend on
Temporalex or call the Temporal client facade directly.

`Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker` is a bounded Oban
dispatcher. It does not contain workflow business logic and never talks to
Temporal directly; it builds a compact start request and calls
`Mezzanine.WorkflowRuntime.start_workflow/1`, which is the only public
Mezzanine Temporal client facade. After Temporal returns a delivery outcome,
the worker records the resulting `workflow_start_outbox` row state through
`Mezzanine.WorkflowRuntime.OutboxPersistence` before it acknowledges,
snoozes, or fails the Oban job.

The row and job args carry refs, hashes, deterministic workflow identity,
authority/decision refs, trace/idempotency scope, release-manifest ref, and
scalar routing metadata only. Raw payloads, Temporal SDK structs, Temporal
protobufs, NIF resources, task tokens, and raw history events stay outside the
public DTO boundary.

`Mezzanine.WorkflowRuntime.WorkflowSignalOutboxWorker` follows the same rule
for retained workflow signals: the local `workflow_signal_outbox` row is the
durable dispatch evidence, Oban only delivers it after commit, and the worker
must persist `dispatch_state`, `workflow_effect_state`, `projection_state`,
attempt count, and error class after each Temporal outcome.

The default outbox persistence store is SQL-backed and targets the execution
repo tables created by `20260420214500_create_workflow_runtime_outboxes.exs`:

```elixir
config :mezzanine_workflow_runtime, :outbox_persistence,
  store: Mezzanine.WorkflowRuntime.OutboxPersistence.SQL,
  repo: Mezzanine.Execution.Repo
```

## Temporalex Runtime Adapter

`Mezzanine.WorkflowRuntime.TemporalexAdapter` is the concrete Temporal-backed
implementation of the `Mezzanine.WorkflowRuntime` facade. Temporal-enabled
environments configure:

```elixir
config :mezzanine_core,
  workflow_runtime_impl: Mezzanine.WorkflowRuntime.TemporalexAdapter
```

The adapter maps Mezzanine start, signal, query, cancel, describe, and history
reference requests into the internal
`Mezzanine.WorkflowRuntime.TemporalexBoundary` and returns only Mezzanine DTOs.
It normalizes Temporalex errors into stable Mezzanine error classes and does
not expose Temporalex handles, protobufs, task tokens, NIF resources, or raw
history events.

`Mezzanine.WorkflowRuntime.TemporalSupervisor` builds Mezzanine-owned
`Temporalex` child specs from `TemporalRegistry`. The default config is inert:

```elixir
config :mezzanine_workflow_runtime, :temporal,
  enabled?: false,
  address: "127.0.0.1:7233",
  namespace: "default"
```

Runtime deployments opt in by setting `enabled?: true`. Local Temporal substrate
control remains repo-owned: use `just dev-up`, `just dev-status`, `just
dev-logs`, `just temporal-ui`, and `just dev-down` from the Mezzanine root.
Do not run raw `temporal server start-dev`, and do not run
`just temporal-reset-confirm` without explicit approval.

## Execution Lifecycle Workflow

`Mezzanine.Workflows.ExecutionAttempt` is the durable execution-attempt
workflow. It compiles Citadel authority, submits the governed Jido lower run,
waits for lower receipt signals when configured, persists the terminal receipt,
then performs terminal workspace cleanup, source publication, evidence
materialization, and review creation through registered activities.

The workflow control policy is deterministic and replay-safe: input-required
lower receipts block for operator review, approval-required receipts fail
closed, max-turn exhaustion stops the loop for finalization or review, stall
timeouts retry or escalate, terminal source state finalizes with cleanup, and
active state continues to the next turn. Operator control signals are
`operator.cancel`, `operator.pause`, `operator.resume`, `operator.retry`,
`operator.replan`, and `operator.rework`.

## Phase 6 Temporal Dispatch Contract

`Mezzanine.WorkflowRuntime.TemporalDispatchContract` owns
`TemporalDispatchContract.v1` evidence for service-mode claims. It joins
Mezzanine-owned Temporal worker specs, the `ExecutionAttempt` workflow on
`mezzanine.hazmat`, compact describe/query refs, restart/replay refs, and
workflow-start outbox outcome persistence. It fails closed when the
`ExecutionAttempt` worker is missing, when a retained start would dispatch to the
wrong task queue, or when a Temporal outcome cannot be persisted locally.

The contract records only refs and DTO summaries. It does not export raw
workflow history, raw payloads, Temporal SDK structs, task tokens, protobufs, or
NIF resources. The ExecutionAttempt workflow also supports an opt-in
`hold_for_receipt?` input for live restart/replay proof: it sets
`accepted_active` state, waits for a `lower_receipt` signal, and resumes with
compact signal state after a worker restart without changing the default
immediate-completion path.

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

`Mezzanine.WorkflowRuntime.DeterministicCodexReceipt` defines the Phase 9
fixture-only Codex receipt activity. It reads a local deterministic receipt
fixture and local Temporal state-file metadata, rejects live provider, Linear,
GitHub, connector, and network adapters before fixture reads, and maps compact
completion, failure, stall, user-input-required, token-dedupe, and rate-limit
facts for WorkflowRuntime reducers. The activity wrapper is
`Mezzanine.Activities.DeterministicCodexReceipt`; default CI remains provider
credential independent.

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

## Workflow Lifecycle Compensation

`Mezzanine.WorkflowRuntime.WorkflowLifecycleCompensation` defines the Phase 5
workflow-lifecycle compensation routing profile. Workflow lifecycle
compensation is routed only as Temporal workflow signals or workflow-owned
activities; `LifecycleContinuation` remains retry/dead-letter visibility and
cannot run owner-command or local mutation callbacks for workflow truth.

The profile builds compact signal/activity requests with compensation refs,
trace/causation/idempotency scope, preconditions, side-effect scope,
dead-letter refs, and audit/evidence refs. Runtime signal dispatch goes
through `Mezzanine.WorkflowRuntime.signal_workflow/1` and strips raw Temporal
SDK/history/task-token data from receipts.

## Active Workflow Truth

`Mezzanine.WorkflowRuntime.ProjectionReconciliation` treats Temporal as the
owner of active workflow lifecycle truth. Postgres execution rows are facts and
operator projections; they cannot authorize terminal workflow closure while
Temporal reports the workflow as active.

`authorize_lifecycle_projection/2` allows a terminal Postgres projection only
when compact Temporal describe/query evidence reports a terminal status and a
terminal workflow event ref. Otherwise the safe action is to signal/cancel the
workflow through `Mezzanine.WorkflowRuntime` or quarantine/repair the
projection.
