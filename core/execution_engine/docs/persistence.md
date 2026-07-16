# Mezzanine Execution Engine Persistence

## Production posture

`Mezzanine.Execution.Store` selects `Store.AshPostgres` when options are
omitted. Production memory profiles are rejected; the deterministic memory
adapter exists only under `test/support` and is not compiled in production.

The production tier is `:postgres_shared`. Executable execution operations live
on `Mezzanine.Execution.ExecutionRecord`; no generic descriptor mutation path
is advertised.

## Preflight

Preflight connects to `Mezzanine.Execution.Repo` and verifies owner migration
`20260428114100` in `schema_migrations`. Caller-supplied proof tokens are not
accepted as substrate evidence.

```elixir
Mezzanine.Execution.Store.preflight()
Mezzanine.Execution.Store.health()
```

Missing migration or database reachability fails before mutation. Execution
health output contains no raw credentials or lower-provider payloads.

## Focused verification

```bash
cd core/execution_engine
mix test test/mezzanine/execution/store_test.exs \
  test/mezzanine/execution/persistence_test.exs
```
