# Mezzanine Projection Engine Persistence

## Production posture

`Mezzanine.Projections.Store` selects `Store.AshPostgres` when options are
omitted. Production memory profiles are rejected; the deterministic memory
adapter exists only under `test/support` and is not compiled in production.

The production tier is `:postgres_shared`. Capability truth is derived from
the AshPostgres adapter and names the executable projection resources.

## Preflight

Preflight connects to `Mezzanine.Projections.Repo` and verifies owner migration
`20260517121000` in `schema_migrations`. Caller-supplied booleans or migration
proof tokens are not accepted as substrate evidence.

```elixir
Mezzanine.Projections.Store.preflight()
Mezzanine.Projections.Store.health()
```

Missing migration or database reachability fails before mutation. Raw secrets,
credential-bearing URLs, prompt bodies, and provider payload bodies remain
forbidden from projection records and health output.

## Focused verification

```bash
cd core/projection_engine
mix test test/mezzanine/projections/store_test.exs \
  test/mezzanine/projections/store_postgres_test.exs \
  test/mezzanine/projections/persistence_test.exs
```
