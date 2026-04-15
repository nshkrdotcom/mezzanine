# Mezzanine Ops Scheduler

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

Internal runtime ownership for due-work selection, claim safety, retry
selection, stall detection, and restart reconciliation.

This package owns OTP processes and scheduler-time reads over the durable
Ash-backed business domains in `core/ops_domain`.

Current modules:

- `Mezzanine.Scheduler`
- `Mezzanine.Scheduler.TickLoop`
- `Mezzanine.Scheduler.WorkSelector`
- `Mezzanine.Scheduler.LeaseManager`
- `Mezzanine.Scheduler.ConcurrencyGate`
- `Mezzanine.Scheduler.RetryQueue`
- `Mezzanine.Scheduler.StallDetector`
- `Mezzanine.Scheduler.ReconcileOnStart`
