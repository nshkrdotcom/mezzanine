# Mezzanine Audit Engine Persistence

## Production posture

`Mezzanine.Audit.Store` selects `Store.AshPostgres` when options are omitted.
Production memory profiles are rejected; the deterministic memory adapter
exists only under `test/support` and is not compiled in production.

The production tier is `:postgres_shared`. Executable audit operations live on
`Mezzanine.Audit.AuditFact` and `Mezzanine.Audit.ExecutionLineageRecord`; no
generic descriptor mutation path is advertised.

## Preflight

Preflight connects to `Mezzanine.Audit.Repo` and verifies owner migration
`20260424170000` in `schema_migrations`. Caller-supplied proof tokens are not
accepted as substrate evidence.

```elixir
Mezzanine.Audit.Store.preflight()
Mezzanine.Audit.Store.health()
```

Missing migration or database reachability fails before mutation. Raw secrets,
prompt bodies, and provider payload bodies remain forbidden from audit state.

## Focused verification

```bash
cd core/audit_engine
mix test test/mezzanine/audit/store_test.exs \
  test/mezzanine/audit/persistence_test.exs
```
