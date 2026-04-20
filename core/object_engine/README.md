# MezzanineObjectEngine

Neutral subject ledger and lifecycle engine for the Mezzanine rebuild.

This package now owns the Phase `2.4.3` durable subject-ledger slice:

- durable `SubjectRecord` persistence
- installation-scoped source refs and identity
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
