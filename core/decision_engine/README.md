# MezzanineDecisionEngine

Neutral decision and review ledger for the Mezzanine rebuild.

## Persistence Posture

Decision ledger state now enters through `Mezzanine.Decisions.Store`. The
default adapter is memory-only. The AshPostgres adapter is explicit durable
opt-in and fails before mutation when migration proof is absent.

This package now owns the Phase `2.4.5` durable decision slice:

- durable `DecisionRecord` persistence
- explicit pending, resolved, waived, escalated, and expired review state
- subject and execution linkage for review truth
- SLA-aware overdue reads for scheduler ownership
- a single terminal-resolution command facade for `decide`, `accept`,
  `reject`, `waive`, `expire`, and `escalate` attempts, backed by
  `DecisionRecord` owner actions and audit-owned attempt evidence

Primary modules:

- `Mezzanine.Decisions`
- `Mezzanine.Decisions.DecisionRecord`
- `Mezzanine.DecisionCommands`
