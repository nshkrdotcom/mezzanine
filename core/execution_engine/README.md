# MezzanineExecutionEngine

Neutral execution ledger and JobOutbox-backed dispatch worker engine for the
Mezzanine rebuild.

This package now owns the Phase `2.4.4` durable execution slice:

- durable `ExecutionRecord` persistence
- durable dispatch identity on `ExecutionRecord`
- substrate-owned dispatch-state and retry metadata
- explicit accepted-but-not-terminal `:awaiting_receipt` state
- stable execution lineage keyed by substrate execution id
- durable lower dispatch through `Mezzanine.JobOutbox` and
  `Mezzanine.ExecutionDispatchWorker`
- frozen lower-facing dispatch snapshots for retry and restart recovery
- lower dedupe lookup before any fresh redispatch attempt
- lower outcome-read seam consumed by lifecycle-side reconcile workers
- neutral control-session reads and ensures through `Mezzanine.WorkControl`
- neutral operator command handling through `Mezzanine.OperatorActions`
- neutral review, waiver, escalation, and gate evaluation through
  `Mezzanine.Reviews`

Primary modules:

- `Mezzanine.Execution`
- `Mezzanine.Execution.ExecutionRecord`
- `Mezzanine.JobOutbox`
- `Mezzanine.LowerGateway`
- `Mezzanine.ExecutionDispatchWorker`
- `Mezzanine.WorkControl`
- `Mezzanine.OperatorActions`
- `Mezzanine.Reviews`
