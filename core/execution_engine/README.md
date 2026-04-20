# MezzanineExecutionEngine

Neutral execution ledger and Temporal workflow handoff engine for the
Mezzanine rebuild.

This package now owns the Phase `2.4.4` durable execution slice:

- durable `ExecutionRecord` persistence
- durable dispatch identity on `ExecutionRecord`
- substrate-owned dispatch-state and retry metadata
- reduced active dispatch states `:queued`, `:in_flight`, and
  `:accepted_active`, with legacy active values retained only as read aliases
  during live-row drains
- stable execution lineage keyed by substrate execution id
- Temporal execution-attempt handoff through `Mezzanine.WorkflowRuntime`
- `Mezzanine.ExecutionDispatchWorker` retained only as an M31 tombstone proving
  the old Oban dispatch worker is retired
- frozen lower-facing dispatch snapshots for retry and restart recovery
- lower dedupe and outcome reads owned by Temporal workflow activities
- neutral control-session reads and ensures through `Mezzanine.WorkControl`
- neutral operator command handling through `Mezzanine.OperatorActions`
- neutral review, waiver, escalation, and gate evaluation through
  `Mezzanine.Reviews`
- review projection payloads include normalized quorum-profile fields from
  `Mezzanine.Review.QuorumProfile` so review gates cannot claim quorum without
  explicit mode, actor, policy, state, and evidence fields
- durable `LifecycleContinuation` records for post-commit lifecycle work that
  must retry, dead-letter, or be waived without recursive transactions

Primary modules:

- `Mezzanine.Execution`
- `Mezzanine.Execution.ExecutionRecord`
- `Mezzanine.LowerGateway`
- `Mezzanine.ExecutionDispatchWorker` (retired tombstone)
- `Mezzanine.WorkControl`
- `Mezzanine.OperatorActions`
- `Mezzanine.Reviews`
- `Mezzanine.Execution.LifecycleContinuation`
