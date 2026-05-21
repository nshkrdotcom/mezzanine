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
