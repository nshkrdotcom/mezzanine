# Monorepo Project Map

- `./bridges/citadel_bridge/mix.exs`: Substrate-origin Citadel governance bridge for Mezzanine run intents
- `./bridges/integration_bridge/mix.exs`: Direct jido_integration bridge for Mezzanine intents
- `./core/archival_engine/mix.exs`: Durable archival manifests and offload contracts for Mezzanine
- `./core/audit_engine/mix.exs`: Operational trace, lineage, and unified audit contracts for Mezzanine
- `./core/barriers/mix.exs`: Durable barrier ledger and exact-close primitives for Mezzanine
- `./core/config_registry/mix.exs`: Durable neutral pack registration and installation registry for Mezzanine
- `./core/decision_engine/mix.exs`: Durable decision and review ledger for Mezzanine
- `./core/evidence_engine/mix.exs`: Durable evidence ledger and completeness helpers for Mezzanine
- `./core/execution_engine/mix.exs`: Durable execution ledger and Temporal workflow handoff contracts for Mezzanine
- `./core/leasing/mix.exs`: Durable leased direct-read and stream-attach substrate for Mezzanine
- `./core/lifecycle_engine/mix.exs`: Durable lifecycle coordinator for explicit mezzanine execution requests
- `./core/mezzanine_core/mix.exs`: Reusable business-semantics substrate for Mezzanine
- `./core/object_engine/mix.exs`: Durable neutral subject ledger and lifecycle ownership for Mezzanine
- `./core/operator_engine/mix.exs`: Operator pause, resume, and cancel substrate for Mezzanine
- `./core/ops_domain/mix.exs`: Durable Ash/AshPostgres business domains for Mezzanine
- `./core/ops_model/mix.exs`: Pure operational vocabulary for the Mezzanine workspace
- `./core/pack_compiler/mix.exs`: Pure validator, compiler, and lifecycle evaluator for Mezzanine packs
- `./core/pack_model/mix.exs`: Typed neutral pack structs for the Mezzanine rebuild
- `./core/projection_engine/mix.exs`: Durable projection rows and materialized views for Mezzanine
- `./core/runtime_scheduler/mix.exs`: Installation-scoped retry timing and restart recovery for Mezzanine
- `./core/workflow_runtime/mix.exs`: Temporal workflow runtime boundary for Mezzanine
- `./mix.exs`: Tooling root for the Mezzanine reusable business-semantics monorepo

# AGENTS.md

## Temporal developer environment

Temporal CLI is implicitly available on this workstation as `temporal` for local durable-workflow development. Do not make repo code silently depend on that implicit machine state; prefer explicit scripts, documented versions, and README-tracked ergonomics work.

## Native Temporal development substrate

When Temporal runtime behavior is required, use the stack substrate in `/home/home/p/g/n/mezzanine`:

```bash
just dev-up
just dev-status
just dev-logs
just temporal-ui
```

Do not invent raw `temporal server start-dev` commands for normal work. Do not reset local Temporal state unless the user explicitly approves `just temporal-reset-confirm`.

<!-- gn-ten:repo-agent:start repo=mezzanine source_sha=ab276c0640772b73065ab12bf05d77be51f1bb67 -->
# mezzanine Agent Instructions Draft

## Owns

- Neutral operational engines.
- Lifecycle coordination.
- Runtime projections.
- Audit, archival, incident bundles, and workflow truth.
- Durable command, outbox, reducer, and operator state.

## Does Not Own

- Product UX.
- Raw provider execution.
- Raw semantic reasoning.
- Connector SDK mechanics.
- Universal lower primitives that belong in GroundPlane.

## Allowed Dependencies

- GroundPlane primitives.
- Citadel authority and governance packets.
- Jido Integration lower gateway contracts.
- AITrace refs for evidence joins.

## Forbidden Imports

- Product modules.
- Provider SDKs for governed execution.
- Ad hoc Temporal dev-server commands.

## Verification

- `mix ci`
- Focused workflow/projection/audit tests for changed mechanisms.

## Escalation

If a missing primitive is universal, move it to GroundPlane. If it is lower
execution, move it to Execution Plane or Jido Integration.
<!-- gn-ten:repo-agent:end -->
