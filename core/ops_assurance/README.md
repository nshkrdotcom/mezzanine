# Mezzanine Ops Assurance

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

`core/ops_assurance` is now a compatibility shim over
`core/execution_engine`'s neutral `Mezzanine.Reviews` service seam:

- decision recording
- gate evaluation
- waiver issuance
- escalation
- release readiness

Durable review resources remain owned by `core/ops_domain`. Live bridge and
proof-harness consumers must bind to `Mezzanine.Reviews` instead of this
package.
