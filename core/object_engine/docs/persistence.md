# Mezzanine Object Engine Persistence

## Production posture

`Mezzanine.Objects.Store` selects `Store.AshPostgres` when options are omitted.
Production memory profiles are rejected; the deterministic memory adapter
exists only under `test/support` and is not compiled in production.

The production tier is `:postgres_shared`. Executable object operations live on
`Mezzanine.Objects.SubjectRecord`; no generic descriptor mutation path is
advertised.

## Preflight

Preflight connects to `Mezzanine.Objects.Repo` and verifies owner migration
`20260426010000` in `schema_migrations`. Caller-supplied proof tokens are not
accepted as substrate evidence.

```elixir
Mezzanine.Objects.Store.preflight()
Mezzanine.Objects.Store.health()
```

Missing migration or database reachability fails before mutation. Object state
and health output contain only bounded safe metadata and opaque refs.

## Focused verification

```bash
cd core/object_engine
mix test test/mezzanine/objects/store_test.exs \
  test/mezzanine/objects/persistence_test.exs
```
