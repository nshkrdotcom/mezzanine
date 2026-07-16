# Mezzanine Archival Engine Persistence

## Production posture

`Mezzanine.Archival.Store` selects `Store.AshPostgres` when options are omitted.
Production memory profiles are rejected; the deterministic memory adapter
exists only under `test/support` and is not compiled in production.

The production tier is `:postgres_shared`. Executable archival operations live
on `Mezzanine.Archival.ArchivalManifest`; no generic descriptor mutation path
is advertised.

## Preflight

Preflight connects to `Mezzanine.Archival.Repo` and verifies owner migration
`20260416103000` in `schema_migrations`. Caller-supplied proof tokens are not
accepted as substrate evidence.

```elixir
Mezzanine.Archival.Store.preflight()
Mezzanine.Archival.Store.health()
```

Missing migration or database reachability fails before mutation. Archive
health output contains no signed URLs, credentials, or raw retained payloads.

## Focused verification

```bash
cd core/archival_engine
mix test test/mezzanine/archival/store_test.exs \
  test/mezzanine/archival/persistence_test.exs
```
