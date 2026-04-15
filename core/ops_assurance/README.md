# Mezzanine Ops Assurance

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

`core/ops_assurance` owns the service seam for review and gate semantics:

- decision recording
- gate evaluation
- waiver issuance
- escalation
- release readiness

Durable review resources remain owned by `core/ops_domain`.
