# MezzanineOperatorEngine

Neutral operator command, diagnostics, and control-room evidence engine for the
Mezzanine rebuild.

This package owns operator-facing commands, diagnostics, intervention
coordination above the neutral execution core, and the Phase 4 control-room
incident bundle contract.

Primary modules:

- `Mezzanine.OperatorCommands`
- `Mezzanine.ExecutionCancelWorker`
- `Mezzanine.ControlRoom.IncidentBundle`

`Mezzanine.ControlRoom.IncidentBundle` implements
`Mezzanine.IncidentBundle.v1` as a compact reference envelope. It requires
tenant, installation, principal or system actor, resource, authority,
idempotency, trace, workflow, lower-fact, semantic, projection, staleness, and
release-manifest references without embedding raw workflow history, lower
payloads, provider metadata, prompts, or artifacts.
