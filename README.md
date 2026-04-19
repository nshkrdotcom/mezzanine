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

Mezzanine is the neutral high-level reusable monorepo behind the nshkr
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
  core/lifecycle_engine # durable lifecycle coordinator over explicit execution requests
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
  core/ops_*           # live semantic hosts pending later neutral rename
  bridges/citadel_bridge
  bridges/integration_bridge
  docs/                # repo-level architecture and publication docs
```

## Scope

- reusable business-semantic engines
- configuration-driven operational machinery
- high-level workflow and policy composition
- generalized models for distributed AI operations
- product-neutral logic above `app_kit`

## Status

The active buildout in this repo is the neutral core scaffold:

- `core/pack_model`
- `core/pack_compiler`
- `core/lifecycle_engine`
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

The `ops_*` packages still host live semantic domains.
They remain frozen to current consumers while later phases move those semantics
into neutral packages with current naming.

Current posture:

- new reusable substrate work lands in the neutral package graph
- dispatch ownership now belongs to `Mezzanine.JobOutbox` plus lower-gateway
  workers, not to a bespoke SQL outbox
- lower-facts reads are tenant-scoped at both lease authorization and
  Jido Integration substrate-read boundaries; `Mezzanine.Leasing` checks the
  caller-carried authorization scope before token validation, and
  `bridges/integration_bridge` forwards only typed `TenantScope` reads to the
  lower store
- internal/operator pack authoring enters through deterministic
  `Mezzanine.Authoring.Bundle` imports; the config registry validates checksum,
  configured signature, policy refs, binding descriptors, lifecycle hints,
  trusted context adapter descriptors, and stale installation revision before
  runtime activation
- the remaining `ops_*` packages are explicit semantic-host carryovers, not a
  reusable extension surface

## Development

The workspace targets Elixir `~> 1.19` and Erlang/OTP `28`.

```bash
mix deps.get
mix ci
```

## Temporal developer environment

Temporal CLI is expected to be available as `temporal` on this developer workstation for local durable-workflow development. Current provisioning is machine-level dotfiles setup, not a repo-local dependency.

TODO: make Temporal ergonomics explicit for developers by adding repo-local setup scripts, version expectations, and fallback instructions so the tool is not silently assumed from the workstation.

## Native Temporal development substrate

Temporal runtime development is managed from `/home/home/p/g/n/mezzanine` through the repo-owned `just` workflow, not by manually starting ad hoc Temporal processes.

Use:

```bash
cd /home/home/p/g/n/mezzanine
just dev-up
just dev-status
just dev-logs
just temporal-ui
```

Expected local contract: `127.0.0.1:7233`, UI `http://127.0.0.1:8233`, namespace `default`, native service `mezzanine-temporal-dev.service`, persistent state `~/.local/share/temporal/dev-server.db`.
