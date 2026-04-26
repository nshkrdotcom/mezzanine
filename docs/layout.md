# Layout

The root is tooling-only.

Current structure:

```text
build_support/
core/pack_model/
core/pack_compiler/
core/lifecycle_engine/
core/config_registry/
core/source_engine/
core/object_engine/
core/workspace_engine/
core/execution_engine/
core/runtime_scheduler/
core/decision_engine/
core/evidence_engine/
core/projection_engine/
core/operator_engine/
core/audit_engine/
core/archival_engine/
core/ops_model/
core/ops_domain/
core/mezzanine_core/
bridges/citadel_bridge/
bridges/integration_bridge/
docs/
packaging/weld/mezzanine_core/
```

The current repo layout is intentionally split into three bands:

- projected neutral core packages under `core/pack_*`, `core/lifecycle_engine/`,
  `core/source_engine/`, `core/workspace_engine/`, `core/*_engine`, and
  `core/config_registry/`
- kept lower bridges in `bridges/citadel_bridge/` and
  `bridges/integration_bridge/`
- still-live semantic hosts pending neutral rename in `core/ops_*/`

The root should remain a workspace owner rather than a runtime home.
