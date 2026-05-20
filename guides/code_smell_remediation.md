# Mezzanine Code Smell Remediation

This guide records the repo-local implementation posture after the GN-TEN code
smell remediation pass.

## What Changed

- Pack compiler support is split into smaller responsibilities instead of one
  monolithic support surface.
- Workflow runtime normalization tables are moved behind narrower model and
  resolver modules.
- Codex CLI lower-runtime adapter concerns remain in adapter zones rather than
  generic workflow paths.
- Mutable runtime state previously stored through `:persistent_term` is owned
  by explicit runtime owners or immutable boot-time configuration.
- Cache invalidation and runtime configuration are documented as explicit
  operational surfaces.
- Workspace command execution is kept behind a named command boundary.

## Maintainer Rules

- Mezzanine owns durable lifecycle and product-neutral business semantics.
- Product or provider facts must arrive as data through packs, bindings,
  receipts, or adapter boundaries.
- Do not add hidden globals, shell execution, or application-env fallback in
  generic engine modules.

## QC

Use the repo root gate:

```bash
mix ci
```
