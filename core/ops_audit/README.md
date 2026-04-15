# Mezzanine Ops Audit

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

`core/ops_audit` is the service seam above the durable evidence domain.

It owns:

- durable audit-event recording helpers
- timeline projection assembly
- evidence-bundle manifest assembly
- report-friendly read models for higher surfaces

It does not own Ash resource definitions. Persistent truth remains in
`core/ops_domain`.
