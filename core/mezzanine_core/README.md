# Mezzanine Core

Reusable business-semantics and canonical run contracts for Mezzanine.

## Durable run truth

`Mezzanine.Repo` owns the Postgres schema for canonical run commands, runs,
first turns, ordered events, read projections, durable cursors, and workflow
start outbox rows. The migration sequence lives in `priv/repo/migrations`.

The production runtime starts the Repo explicitly and selects
`Mezzanine.WorkflowRuntime.Store.Postgres`. `Mezzanine.Core.Application` does
not start a memory store. Production readiness must call the store preflight,
which reaches Postgres and verifies migration `20260715100000`.

The frozen public command/result types are under `Mezzanine.Runs`. They contain
only opaque refs, hashes, scalar state, and bounded metadata.

See `docs/persistence.md` for the package persistence contract.
