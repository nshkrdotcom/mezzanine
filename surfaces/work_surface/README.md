# Mezzanine Work Surface

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the frozen legacy northbound surface layer. It remains
buildable only during the coexistence window guarded by
`NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE` and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

Reusable northbound work-intake and work-status surface for Mezzanine.

This package owns:

- external work normalization and idempotent intake
- work detail assembly
- queue statistics
- work status projections

It stays above durable domain truth and below product-specific shells.
