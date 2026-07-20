# Mezzanine Core

Reusable business-semantics and canonical run contracts for Mezzanine.

## Durable run contracts

This package owns the frozen command, event, cursor, acceptance, and workflow
handoff types under `Mezzanine.Runs`. It owns no database or run lifecycle.

Canonical `WorkObject`, `WorkPlan`, `RunSeries`, and `Run` persistence belongs
to `Mezzanine.OpsDomain.Repo`. The workflow runtime adds first-turn, ordered
event, projection, cursor, and workflow-outbox records in that same owner-local
database transaction. Production readiness calls the workflow store preflight,
which verifies the Ops Domain migration `20260720111500`.

The frozen public command/result types are under `Mezzanine.Runs`. They contain
only opaque refs, hashes, scalar state, and bounded metadata.

See `docs/persistence.md` for the ownership contract.
