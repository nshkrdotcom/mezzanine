# MezzanineArchivalEngine

Neutral archival engine for the Mezzanine rebuild.

This package now owns the durable Stage-11 substrate archival slice for:

- terminal-subject discovery and scheduler-driven archival
- durable archival manifests carrying subject graph membership and cold-storage state
- filesystem-backed cold snapshots plus archived-query helpers
- hot-row removal and archival telemetry on the real scheduler path

Primary public surfaces:

- `Mezzanine.Archival.ArchivalManifest`
- `Mezzanine.Archival.ColdStore`
- `Mezzanine.Archival.Query`

Internal implementation files:

- `lib/mezzanine/archival/cold_store.ex`
- `lib/mezzanine/archival/scheduler.ex`
- `lib/mezzanine/archival/snapshot.ex`
