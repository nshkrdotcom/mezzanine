# MezzanineArchivalEngine

Neutral archival engine for the Mezzanine rebuild.

This package now owns the durable Stage-11 substrate archival slice for:

- terminal-subject discovery and scheduler-driven archival
- durable archival manifests carrying subject graph membership and cold-storage state
- filesystem-backed cold snapshots plus archived-query helpers
- hot-row removal and archival telemetry on the real scheduler path
- archived trace source lookup by trace, subject, execution, decision, run,
  attempt, artifact, and manifest pivots
- operator-facing staleness labels that distinguish archived truth from hot,
  lower-fresh, projection-stale, diagnostic, or unavailable fields
- Phase 4 release contracts for cold restore by trace id, cold restore by
  artifact id, hot/cold conflict quarantine, and archival sweep retry
  quarantine
- audit inclusion proof joins for restore evidence, with diagnostic quarantine
  when tenant/installation, trace, audit fact, checkpoint, release manifest,
  and restore-consistency evidence cannot be joined

Primary public surfaces:

- `Mezzanine.Archival.ArchivalManifest`
- `Mezzanine.Archival.ArchivalConflict`
- `Mezzanine.Archival.ArchivalSweep`
- `Mezzanine.Archival.ColdStore`
- `Mezzanine.Archival.ColdRestoreArtifactQuery`
- `Mezzanine.Archival.ColdRestoreTraceQuery`
- `Mezzanine.Archival.Query`
- `Mezzanine.Archival.RestoreAuditJoin`

Internal implementation files:

- `lib/mezzanine/archival/cold_store.ex`
- `lib/mezzanine/archival/scheduler.ex`
- `lib/mezzanine/archival/snapshot.ex`
