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
- `Mezzanine.ControlRoom.IncidentExportBundle`

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
