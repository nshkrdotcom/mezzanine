# MezzanineArchivalEngine

Neutral archival engine for the Mezzanine rebuild.

This package now freezes the Phase `2.3` archival contract for:

- terminal graph eligibility
- archival countdown policy
- manifest-before-delete discipline
- hot-to-cold offload planning for terminal subject graphs

Primary modules:

- `Mezzanine.Archival.CountdownPolicy`
- `Mezzanine.Archival.Graph`
- `Mezzanine.Archival.Manifest`
- `Mezzanine.Archival.OffloadPlan`
