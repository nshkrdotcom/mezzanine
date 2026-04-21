# MezzanineOperatorEngine

Neutral operator command, diagnostics, and control-room evidence engine for the
Mezzanine rebuild.

This package owns operator-facing commands, diagnostics, intervention
coordination above the neutral execution core, and the Phase 4 control-room
incident bundle contract.

Operator pause, resume, and cancel commands now delegate durable subject status
mutation to `Mezzanine.Objects.SubjectRecord` and execution cancellation to
`Mezzanine.Execution.ExecutionRecord`, while retaining workflow signal
actions for `Mezzanine.WorkflowRuntime`. Each returned operator effect is
classified by `Mezzanine.Execution.OperatorActionClassification` as either a
workflow signal or a declared local mutation. Workflow-owned accepted
executions are signaled instead of locally terminalized; local execution,
subject, and lease mutations name their bounded-context owner. Commands do not
mutate old Oban saga jobs or enqueue lower-cancel workers.

Primary modules:

- `Mezzanine.OperatorCommands`
- `Mezzanine.ControlRoom.ForensicReplay`
- `Mezzanine.ControlRoom.IncidentBundle`
- `Mezzanine.ControlRoom.IncidentExportBundle`
- `Mezzanine.ControlRoom.QueuePressurePolicy`
- `Mezzanine.ControlRoom.RetryPosture`
- `Mezzanine.ControlRoom.SuppressionVisibility`

`Mezzanine.ControlRoom.IncidentBundle` implements
`Mezzanine.IncidentBundle.v1` as a compact reference envelope. It requires
tenant, installation, principal or system actor, resource, authority,
idempotency, trace, workflow, lower-fact, semantic, projection, staleness, and
release-manifest references without embedding raw workflow history, lower
payloads, provider metadata, prompts, or artifacts.

`Mezzanine.ControlRoom.IncidentExportBundle` implements
`Mezzanine.IncidentExportBundle.v1` for operator-downloadable incident exports.
It requires tenant, authority, trace, export, incident, included-reference,
redaction-manifest, checksum, operator, format, and release-manifest evidence,
and rejects raw workflow history, lower payloads, provider bodies, prompts,
artifacts, and tenant-sensitive secrets.

`Mezzanine.ControlRoom.ForensicReplay` implements
`Mezzanine.ForensicReplay.v1` as a compact replay timeline and integrity
report over incident evidence refs. It requires tenant, installation,
workspace, project, environment, principal or system actor, resource,
authority, idempotency, trace, release-manifest, incident, timeline, ordered
event, integrity-hash, missing-ref-set, and replay-result evidence, and rejects
raw workflow history, lower payloads, provider bodies, prompts, artifacts, and
tenant-sensitive secrets.

`Mezzanine.ControlRoom.QueuePressurePolicy` implements
`Mezzanine.QueuePressurePolicy.v1` for retained local queues such as the
workflow-start outbox, signal outbox, claim-check GC, and bounded local jobs.
It requires tenant, installation, workspace, project, environment,
principal-or-system-actor, resource, authority, idempotency, trace, release
manifest, queue, budget, threshold, pressure, admission, shed decision, retry
delay, and operator-message evidence before queue pressure can be surfaced or
counted as deterministic shedding.

`Mezzanine.ControlRoom.RetryPosture` implements `Platform.RetryPosture.v1` for
workflow, activity, lower integration, and retained local-job failure paths. It
requires owner repo, producer, consumer, operation, retry class, failure class,
bounded attempts, backoff policy, idempotency scope, dead-letter ref, safe
action, and the same enterprise scope refs as other Phase 4 contracts.

`Mezzanine.ControlRoom.SuppressionVisibility` implements
`Platform.SuppressionVisibility.v1` for operator-visible suppression and
quarantine records. It requires tenant, installation, workspace, project,
environment, principal-or-system-actor, resource, authority, idempotency,
trace, release manifest, suppression, target, reason, diagnostics, and at
least one recovery action ref before hidden work can be treated as safely
visible to operators.
