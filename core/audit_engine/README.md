# MezzanineAuditEngine

Neutral audit ledger and timeline engine for the Mezzanine rebuild.

This package now owns the Phase `2.4.2` durable audit-ledger slice and retains
the Phase `2.3` operational contract for:

- durable `AuditFact` persistence
- audit-owned append and read helpers for aggregate-safe audit fact writes and
  classifier reads
- audit-owned inclusion/checkpoint proof evidence for audit facts, with Merkle
  terminology reserved for explicit Merkle proof inputs whose root can be
  recomputed from the fact hash and sibling path
- neutral work-audit timeline and evidence-bundle services used by bounded
  northbound consumers
- durable execution-lineage persistence keyed by substrate execution id
- indexed `trace_id` and `causation_id` join keys
- substrate-owned execution lineage vs lower-owned internal identifiers
- truth-precedence and staleness classes for enriched operator views
- Phase 4 tenant-scoped trace-join evidence for `Platform.TenantScopedTraceJoin.v1`
- pure unified-trace assembly for the operator-facing “3 AM query”

Primary modules:

- `Mezzanine.Audit.TraceContract`
- `Mezzanine.Audit.AuditAppend`
- `Mezzanine.Audit.AuditFact`
- `Mezzanine.Audit.AuditInclusionProof`
- `Mezzanine.Audit.AuditQuery`
- `Mezzanine.Audit.ExecutionLineage`
- `Mezzanine.Audit.ExecutionLineageStore`
- `Mezzanine.Audit.WorkAudit`
- `Mezzanine.Audit.Staleness`
- `Mezzanine.Audit.TenantScopedTraceJoin`
- `Mezzanine.Audit.UnifiedTrace`
