# Layout

The root is tooling-only.

Current structure:

```text
build_support/
core/pack_model/
core/pack_compiler/
core/config_registry/
core/object_engine/
core/execution_engine/
core/runtime_scheduler/
core/decision_engine/
core/evidence_engine/
core/projection_engine/
core/operator_engine/
core/audit_engine/
core/archival_engine/
core/ops_model/
core/ops_policy/
core/ops_planner/
core/ops_domain/
core/ops_audit/
core/ops_control/
core/ops_assurance/
core/ops_scheduler/
core/mezzanine_core/
bridges/app_kit_bridge/
bridges/citadel_bridge/
bridges/integration_bridge/
surfaces/work_surface/
surfaces/operator_surface/
surfaces/review_surface/
surfaces/program_surface/
docs/
packaging/weld/mezzanine_core/
```

The current repo layout is intentionally split into three bands:

- projected neutral core packages under `core/pack_*`, `core/*_engine`, and
  `core/config_registry/`
- kept lower bridges in `bridges/citadel_bridge/` and
  `bridges/integration_bridge/`
- frozen legacy migration scaffolding in `core/ops_*/`, `bridges/app_kit_bridge/`,
  and `surfaces/*`

The root should remain a workspace owner rather than a runtime home.
