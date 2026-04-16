# MezzanineExecutionEngine

Neutral execution ledger and dispatch outbox engine for the Mezzanine rebuild.

This package now owns the Phase `2.4.4` durable execution slice:

- durable `ExecutionRecord` persistence
- durable `DispatchOutboxEntry` truth
- substrate-owned dispatch-state and retry metadata
- stable execution-to-outbox lineage linkage

Primary modules:

- `Mezzanine.Execution`
- `Mezzanine.Execution.ExecutionRecord`
- `Mezzanine.Execution.DispatchOutboxEntry`
