# MezzanineDecisionEngine

Neutral decision and review ledger for the Mezzanine rebuild.

This package now owns the Phase `2.4.5` durable decision slice:

- durable `DecisionRecord` persistence
- explicit pending, resolved, waived, escalated, and expired review state
- subject and execution linkage for review truth
- SLA-aware overdue reads for scheduler ownership

Primary modules:

- `Mezzanine.Decisions`
- `Mezzanine.Decisions.DecisionRecord`
