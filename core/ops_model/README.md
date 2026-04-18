# Mezzanine Ops Model

Status: `Current semantic host pending neutral rename`

This package still carries typed semantic structs consumed by
`core/ops_domain` while later phases move those types into neutral package
names and boundaries.

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
