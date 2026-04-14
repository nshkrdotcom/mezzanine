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
  core/mezzanine_core  # initial reusable core package
  docs/                # repo-level architecture and publication docs
```

## Scope

- reusable business-semantic engines
- configuration-driven operational machinery
- high-level workflow and policy composition
- generalized models for distributed AI operations
- product-neutral logic above `app_kit`

## Status

Initial tooling-root workspace scaffold with Blitz and Weld ready from day one.

## Development

The workspace targets Elixir `~> 1.19` and Erlang/OTP `28`.

```bash
mix deps.get
mix ci
```
