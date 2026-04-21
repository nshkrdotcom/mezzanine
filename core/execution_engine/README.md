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
- review decision recording consults `Mezzanine.Review.QuorumResolver` before
  mutating `ReviewUnit` terminal state, so non-single approval modes consume
  persisted `ReviewDecision` rows as resolver inputs instead of treating one
  decision append as terminal truth
- review quorum resolution includes `Mezzanine.Review.ActorCountingPolicy`, so
  one actor counts once for quorum and multi-role counting remains fail-closed
  unless ops-domain source registers a specific authority policy
- durable `LifecycleContinuation` records for post-commit lifecycle work that
  must retry, dead-letter, or be waived without recursive transactions
- `LifecycleContinuation.process/2` dispatch restricted to declared
  owner-command or workflow-signal targets recorded on the continuation;
  anonymous callback handlers are rejected before claim
- source-owned owner-directed compensation profile fields via
  `Mezzanine.Execution.OwnerDirectedCompensation`; this is a validation
  profile, not a saga runner or multi-context rollback worker
- bounded-context repair routing through
  `Mezzanine.Execution.BoundedContextRepairRouting`, which accepts only named
  owner-command targets for execution, decision, audit, archival, lower, and
  projection repair while keeping `LifecycleContinuation` as retry/dead-letter
  visibility only
- compensation retry, dead-letter, and operator action evidence through
  `Mezzanine.Execution.CompensationEvidence`, which rejects silent retry loops,
  missing operator evidence, hidden rollback callbacks, raw payloads, task
  tokens, and lifecycle-continuation-handler repair targets
- operator effects classified by
  `Mezzanine.Execution.OperatorActionClassification` as either workflow
  signals through `Mezzanine.WorkflowRuntime.signal_workflow/1` or declared
  local mutations owned by a bounded context; unclassified refs, callbacks,
  raw SQL writes, old Oban saga jobs, and lower-cancel workers fail closed

Primary modules:

- `Mezzanine.Execution`
- `Mezzanine.Execution.ExecutionRecord`
- `Mezzanine.LowerGateway`
- `Mezzanine.ExecutionDispatchWorker` (retired tombstone)
- `Mezzanine.WorkControl`
- `Mezzanine.OperatorActions`
- `Mezzanine.Reviews`
- `Mezzanine.Execution.LifecycleContinuation`
- `Mezzanine.Execution.OwnerDirectedCompensation`
- `Mezzanine.Execution.BoundedContextRepairRouting`
- `Mezzanine.Execution.CompensationEvidence`
- `Mezzanine.Execution.OperatorActionClassification`
