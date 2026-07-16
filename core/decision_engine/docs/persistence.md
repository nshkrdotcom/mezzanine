# Mezzanine Decision Engine Persistence

## Production posture

`Mezzanine.Decisions.Store` selects `Store.AshPostgres` when options are
omitted. Production memory profiles are rejected; the deterministic memory
adapter exists only under `test/support` and is not compiled in production.

The production tier is `:postgres_shared`. Executable decision operations live
on `Mezzanine.Decisions.DecisionRecord` and `Mezzanine.DecisionCommands`; no
generic descriptor mutation path is advertised.

## Preflight

Preflight connects to `Mezzanine.Decisions.Repo` and verifies owner migration
`20260419010000` in `schema_migrations`. Caller-supplied proof tokens are not
accepted as substrate evidence.

```elixir
Mezzanine.Decisions.Store.preflight()
Mezzanine.Decisions.Store.health()
```

Missing migration or database reachability fails before mutation. Decision
health output contains no raw authority or secret material.

## Focused verification

```bash
cd core/decision_engine
mix test test/mezzanine/decisions/store_test.exs \
  test/mezzanine/decisions/persistence_test.exs
```
