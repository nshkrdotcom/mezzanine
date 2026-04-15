<p align="center">
  <img src="assets/mezzanine.svg" width="200" height="200" alt="Mezzanine logo" />
</p>

<p align="center">
  <a href="https://github.com/nshkrdotcom/mezzanine/actions/workflows/ci.yml">
    <img alt="GitHub Actions Workflow Status" src="https://github.com/nshkrdotcom/mezzanine/actions/workflows/ci.yml/badge.svg" />
  </a>
  <a href="https://github.com/nshkrdotcom/mezzanine/blob/main/LICENSE">
    <img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-0b0f14.svg" />
  </a>
</p>

# Mezzanine

Mezzanine is the future neutral high-level reusable monorepo behind the nshkr
product stack.

It is the place for generalized business semantics, configurable operational
engines, Ash-shaped domain logic, and reusable application-layer machinery that
should not live in product repos like `extravaganza` and should not be forced
down into lower infrastructure layers like `app_kit`, `citadel`, or
`jido_integration`.

## Current Layout

```text
mezzanine/
  build_support/       # workspace and Weld manifests
  core/mezzanine_core  # projected artifact shell over the neutral rebuild
  core/pack_model      # neutral pack definitions and shared structs
  core/pack_compiler   # neutral pack compilation and validation
  core/config_registry # deployment/install registry seam
  core/object_engine   # subject/object lifecycle engine scaffold
  core/execution_engine
  core/runtime_scheduler
  core/decision_engine
  core/evidence_engine
  core/projection_engine
  core/operator_engine
  core/audit_engine
  core/archival_engine
  core/ops_*           # [DEPRECATED-PENDING-MIGRATION] legacy ontology packages
  bridges/app_kit_bridge
                       # [DEPRECATED-PENDING-MIGRATION] frozen legacy northbound bridge
  bridges/citadel_bridge
  bridges/integration_bridge
  bridges/execution_plane_bridge
  surfaces/*           # [DEPRECATED-PENDING-MIGRATION] frozen legacy northbound surfaces
  docs/                # repo-level architecture and publication docs
```

## Scope

- reusable business-semantic engines
- configuration-driven operational machinery
- high-level workflow and policy composition
- generalized models for distributed AI operations
- product-neutral logic above `app_kit`

## Status

The repo is in the bounded coexistence phase from the v3 packet.

The active buildout in this repo is the neutral core scaffold:

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

The legacy `ops_*` packages, `bridges/app_kit_bridge`, and `surfaces/*`
packages remain buildable only as migration scaffolding and are
`[DEPRECATED-PENDING-MIGRATION]`.

Named coexistence gates:

- `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
- `MEZZANINE_NEUTRAL_CORE_CUTOVER`

During this phase:

- no new product or `app_kit` dependency may target the deprecated ontology
- the legacy northbound surfaces stay frozen
- the neutral packages are the only place new reusable substrate work may land

## Development

The workspace targets Elixir `~> 1.19` and Erlang/OTP `28`.

```bash
mix deps.get
mix ci
```
