# MezzanineProjectionEngine

Neutral projection engine for the Mezzanine rebuild.

This package now owns the durable `2.4.6` substrate read-model slice for:

- named projection rows with indexed `trace_id` / `causation_id` joins
- async `MaterializedProjection` snapshots for non-interactive views
- lower-receipt reduction into execution, subject, decision, evidence,
  projection, and audit ledgers through `Mezzanine.Projections.ReceiptReducer`
- source drift reconciliation rows for terminal sources, missing or reassigned
  provider objects, blockers, stale polls, and out-of-band source updates
- review-gate projection behavior for accept, reject/rework, waive, expire,
  and escalate decisions through `Mezzanine.Projections.ReviewGate`
- package-local migrations and tests proving durable projection ownership

Primary modules:

- `Mezzanine.Projections.ProjectionRow`
- `Mezzanine.Projections.MaterializedProjection`
- `Mezzanine.Projections.ReceiptReducer`
- `Mezzanine.Projections.SourceReconciliation`
- `Mezzanine.Projections.ReviewGate`

The service modules consume ids and refs already carried by source admission,
workflow state, lower receipts, decision records, or explicit operator actions.
They do not read process environment and they do not accept static provider
object selectors such as GitHub issue numbers or Linear issue ids as a
production path.
