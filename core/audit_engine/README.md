# MezzanineAuditEngine

Neutral audit ledger and timeline engine for the Mezzanine rebuild.

This package now freezes the Phase `2.3` operational contract for:

- indexed `trace_id` and `causation_id` join keys
- substrate-owned execution lineage vs lower-owned internal identifiers
- truth-precedence and freshness classes for enriched operator views
- pure unified-trace assembly for the operator-facing “3 AM query”

Primary modules:

- `Mezzanine.Audit.TraceContract`
- `Mezzanine.Audit.ExecutionLineage`
- `Mezzanine.Audit.Freshness`
- `Mezzanine.Audit.UnifiedTrace`
