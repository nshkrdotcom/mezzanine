# Mezzanine Ops Planner

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

Pure planning and obligation derivation for the Mezzanine workspace.

This package takes typed operational model structs plus compiled policy bundles
and produces plan artifacts without Ash, OTP processes, or lower-level effects.

Current responsibilities:

- compile `WorkObject + PolicyBundle` into a `WorkPlan`
- derive review obligations from the compiled plan
- compute retry schedules
- answer dependency-readiness questions
