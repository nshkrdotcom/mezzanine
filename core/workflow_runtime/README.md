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
