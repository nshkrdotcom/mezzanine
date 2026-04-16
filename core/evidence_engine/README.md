# MezzanineEvidenceEngine

Neutral evidence ledger and completeness helpers for the Mezzanine rebuild.

This package now owns the Phase `2.4.5` durable evidence slice:

- durable `EvidenceRecord` persistence
- subject and execution lineage for collected review evidence
- explicit pending, collected, verified, and failed evidence state
- completeness helpers over the neutral ledger without projection-only shortcuts

Primary modules:

- `Mezzanine.EvidenceLedger`
- `Mezzanine.EvidenceLedger.EvidenceRecord`
- `Mezzanine.EvidenceLedger.Summary`
