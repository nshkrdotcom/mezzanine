# MezzanineArchivalEngine

Neutral archival engine for the Mezzanine rebuild.

This package now owns the durable `2.4.6` substrate archival slice for:

- terminal graph countdown and manifest planning contracts
- durable archival manifests carrying graph membership and due times
- cold-storage completion metadata and hot-delete eligibility checks

Primary modules:

- `Mezzanine.Archival.CountdownPolicy`
- `Mezzanine.Archival.Graph`
- `Mezzanine.Archival.Manifest`
- `Mezzanine.Archival.OffloadPlan`
- `Mezzanine.Archival.ArchivalManifest`
