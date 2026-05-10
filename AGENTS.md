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
- `./core/optimization_engine/mix.exs`: Governed GEPA orchestration, evaluator pool, budgets, and promotion gates
- `./core/ops_domain/mix.exs`: Durable Ash/AshPostgres business domains for Mezzanine
- `./core/ops_model/mix.exs`: Pure operational vocabulary for the Mezzanine workspace
- `./core/pack_compiler/mix.exs`: Pure validator, compiler, and lifecycle evaluator for Mezzanine packs
- `./core/pack_model/mix.exs`: Typed neutral pack structs for the Mezzanine rebuild
- `./core/projection_engine/mix.exs`: Durable projection rows and materialized views for Mezzanine
- `./core/runtime_scheduler/mix.exs`: Installation-scoped retry timing and restart recovery for Mezzanine
- `./core/workflow_runtime/mix.exs`: Temporal workflow runtime boundary for Mezzanine
- `./mix.exs`: Tooling root for the Mezzanine reusable business-semantics monorepo

# AGENTS.md

## Onboarding

Read `ONBOARDING.md` first for the repo's one-screen ownership, first command,
and proof path.

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

## Dependency Sources

- Dependency source selection is handled by `build_support/dependency_sources.exs` and `build_support/dependency_sources.config.exs`.
- Local dependency overrides use `.dependency_sources.local.exs`.
- Dependency source selection must not use environment variables.
- Same-repo workspace package paths may stay in their local `mix.exs` files; cross-repo dependencies that need fallback behavior belong in the dependency-source manifest.
- Weld checks helper drift, dependency-source manifests, clone/publish checks, and publish order for this repo; keep the committed dependency on the released Hex Weld line.

## Runtime Env

- Runtime application code under `lib/**`, package `lib/**`, example `lib/**`, and Mix task modules must not call direct OS env APIs such as `System.get_env`, `System.fetch_env`, `System.put_env`, or `System.delete_env`.
- Runtime/deployment env reads belong in `config/runtime.exs` or a `Config.Provider`.
- Mix tasks, examples, and harnesses should accept explicit flags, app config, or caller-supplied env maps instead of reading or mutating process env.

## Live Provider Checks

For live provider checks, use `~/scripts/with_bash_secrets <command>`. It sources
`~/.bash/bash_secrets` and execs the command. Do not print secret values. Pipe
`LINEAR_API_KEY` via stdin for Linear examples. GitHub live examples use `gh auth`
or `GH_TOKEN`/`GITHUB_TOKEN` from the wrapper. Codex SDK examples use the existing
Codex/OpenAI machine auth through the wrapper. Live provider smoke is not product
acceptance unless it runs the product-owned Extravaganza command path.

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

## Blitz 0.3.0 operational note

Root workspace Blitz uses published Hex `~> 0.3.0` by default; `.blitz/` is committed compact impact state after green QC. Source and `mix.exs` changes cascade through reverse workspace dependencies; docs-only changes should stay owner-local.
