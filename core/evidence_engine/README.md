# MezzanineEvidenceEngine

Neutral evidence ledger and completeness helpers for the Mezzanine rebuild.

## Persistence Posture

Evidence ledger state now enters through `Mezzanine.EvidenceLedger.Store`. The
default adapter is memory-only. The AshPostgres adapter is adapter-local and
requires explicit durable profile selection with migration proof.

This package now owns the Phase `2.4.5` durable evidence slice:

- durable `EvidenceRecord` persistence
- subject and execution lineage for collected review evidence
- explicit pending, collected, verified, and failed evidence state
- completeness helpers over the neutral ledger without projection-only shortcuts

Primary modules:

- `Mezzanine.EvidenceLedger`
- `Mezzanine.EvidenceLedger.EvidenceRecord`
- `Mezzanine.EvidenceLedger.Summary`

## Persistence Documentation

See `docs/persistence.md` for tiers, defaults, adapters, unsupported selections, config examples, restart claims, durability claims, debug sidecar behavior, redaction guarantees, migration or preflight behavior, and no-bypass scope when applicable.
