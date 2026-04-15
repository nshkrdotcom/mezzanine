# MezzanineCore

Reusable business-semantics substrate for the Mezzanine workspace.

This projected package boundary now sits above the neutral rebuild packages:

- `core/pack_model`
- `core/pack_compiler`
- `core/config_registry`
- `core/object_engine`
- `core/execution_engine`
- `core/runtime_scheduler`
- `core/decision_engine`
- `core/evidence_engine`
- `core/projection_engine`
- `core/operator_engine`
- `core/audit_engine`
- `core/archival_engine`

The legacy `ops_*` packages and `surfaces/*` remain outside this projected
artifact and are `[DEPRECATED-PENDING-MIGRATION]` migration scaffolding only.

`mezzanine_core` continues to project the reusable neutral substrate rather than
the legacy product-shaped ontology.
