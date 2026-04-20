# Mezzanine Ops Domain

Status: `Current semantic host pending neutral rename`

This package still owns live program, work, run, review, evidence, and control
domains while later phases migrate those semantics into neutral package names
and boundaries.

Postgres-backed durable business domains for the Mezzanine workspace.

## Scope

This package owns the first Ash/AshPostgres-backed durable truth for:

- programs
- policy bundles
- work classes
- work objects
- work plans
- run series and runs
- run grants and run artifacts
- review units, decisions, waivers, and escalations
- explicit review quorum profile field normalization for review-unit
  `decision_profile` metadata
- source-owned review quorum close-behavior specification for supported modes,
  without changing terminal resolver behavior
- evidence bundles, evidence items, audit events, and timeline projections
- operator control sessions and interventions

It does not own:

- pure policy compilation
- pure planning
- scheduling
- lower execution
- product-facing UI surfaces

## Development

```bash
mix deps.get
mix ash.setup
mix test
```
