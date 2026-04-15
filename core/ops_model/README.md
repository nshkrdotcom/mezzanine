# Mezzanine Ops Model

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

Pure operational vocabulary for the Mezzanine workspace.

This package is intentionally data-only. It defines:

- first-class semantic structs like `WorkObject`, `WorkPlan`, `Run`, and
  `PolicyBundle`
- pure intent structs for higher-order lowering
- canonical state vocabularies
- deep normalization helpers for external payloads

It must stay free of:

- Ash
- OTP processes
- external I/O
- lower runtime or integration coupling
