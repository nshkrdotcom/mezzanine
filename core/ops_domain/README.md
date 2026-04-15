# Mezzanine Ops Domain

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

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
