# MezzanineProjectionEngine

Neutral projection engine for the Mezzanine rebuild.

This package now owns the durable `2.4.6` substrate read-model slice for:

- named projection rows with indexed `trace_id` / `causation_id` joins
- async `MaterializedProjection` snapshots for non-interactive views
- package-local migrations and tests proving durable projection ownership

Primary modules:

- `Mezzanine.Projections.ProjectionRow`
- `Mezzanine.Projections.MaterializedProjection`
