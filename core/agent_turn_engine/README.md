# Mezzanine Agent Turn Engine

Pure agent turn ledger, replay, cursor, pending interaction, and reducer
contracts.

This package owns Mezzanine's native agent turn truth. It is data and pure
functions only: no database, no processes, no AppKit DTOs, no Jido connector
internals, no Execution Plane process internals, no generated protocol modules,
and no sidecar runtime integration.

Products enter through AppKit. Lower effects are authorized by Citadel and
executed through the governed lower dispatch layers. This package records and
reduces refs, summaries, redaction posture, idempotency facts, and replay-safe
ledger state.

## Store and Replay

`Mezzanine.AgentTurnEngine.Store` defines the persistence boundary for ledgers,
events, pending decisions, cursors, replay records, and projection rows.
`Mezzanine.AgentTurnEngine.Store.Memory` is the package-local implementation
used for deterministic tests and host adapters that need caller-owned state. It
does not start processes, call lower runtimes, or own durable storage.

Replay and cursor catch-up only read existing facts. A retry of a lower effect
requires an explicit `:retry_lower_effect` replay record with evidence refs, and
the memory adapter still does not dispatch the lower work itself.

## Projection Rows

`Mezzanine.AgentTurnEngine.Projection` reduces conversation events into
product-safe rows. Rows carry summaries, payload refs, redaction class,
authority refs, and evidence refs; provider/runtime payloads stay outside this
package.
