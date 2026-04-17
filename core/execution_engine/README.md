# MezzanineExecutionEngine

Neutral execution ledger and dispatch outbox engine for the Mezzanine rebuild.

This package now owns the Phase `2.4.4` durable execution slice:

- durable `ExecutionRecord` persistence
- durable `DispatchOutboxEntry` truth
- substrate-owned dispatch-state and retry metadata
- stable execution-to-outbox lineage linkage
- durable dispatch claim and classification through
  `Mezzanine.Execution.Dispatcher`
- frozen lower-facing dispatch snapshots for retry and restart recovery
- post-acceptance semantic-failure reconciliation without reopening the outbox
- neutral control-session reads and ensures through `Mezzanine.WorkControl`
- neutral operator command handling through `Mezzanine.OperatorActions`
- neutral review, waiver, escalation, and gate evaluation through
  `Mezzanine.Reviews`

Primary modules:

- `Mezzanine.Execution`
- `Mezzanine.Execution.ExecutionRecord`
- `Mezzanine.Execution.DispatchOutboxEntry`
- `Mezzanine.Execution.Dispatcher`
- `Mezzanine.WorkControl`
- `Mezzanine.OperatorActions`
- `Mezzanine.Reviews`
