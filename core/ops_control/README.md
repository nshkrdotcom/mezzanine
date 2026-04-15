# Mezzanine Ops Control

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

`core/ops_control` owns the service seam for operator control:

- pause and resume
- cancellation
- replan requests
- grant overrides

Persistent state still lives in `core/ops_domain`; this package coordinates
those transitions coherently and records durable audit events.
