# MezzanineObjectEngine

Neutral subject ledger and lifecycle engine for the Mezzanine rebuild.

## Persistence Posture

Object lifecycle state now enters through `Mezzanine.Objects.Store`. The
default and only production adapter is AshPostgres. Omitted options select it,
live preflight verifies the owner migration, and memory profiles are rejected.
The deterministic memory adapter is compiled only from `test/support`.

This package now owns the Phase `2.4.3` durable subject-ledger slice:

- durable `SubjectRecord` persistence
- installation-scoped source refs and identity
- provider source metadata for source event ID, source binding ID, provider
  external ref, provider revision, source state, labels, priority, branch/ref
  URL, source routing, blocker refs, and workpad/progress refs
- source-owned payload schema binding before ingest accepts a payload map
- deterministic rejection/quarantine refs for missing, unknown, stale, or
  future subject payload schema identity
- canonical `lifecycle_state` ownership
- optimistic row-versioning for lifecycle updates
- subject block/unblock rescue overlay

Primary modules:

- `Mezzanine.Objects`
- `Mezzanine.Objects.SubjectRecord`
- `Mezzanine.Objects.SubjectPayloadSchema`

## Persistence Documentation

See `docs/persistence.md` for tiers, defaults, adapters, unsupported selections, config examples, restart claims, durability claims, debug sidecar behavior, redaction guarantees, migration or preflight behavior, and no-bypass scope when applicable.
