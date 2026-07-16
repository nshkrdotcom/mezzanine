# Mezzanine Evidence Engine Persistence

## Production posture

`Mezzanine.EvidenceLedger.Store` selects `Store.AshPostgres` when options are
omitted. Production memory profiles are rejected; the deterministic memory
adapter exists only under `test/support` and is not compiled in production.

The production tier is `:postgres_shared`. Executable evidence operations live
on `Mezzanine.EvidenceLedger.EvidenceRecord`; no generic descriptor mutation
path is advertised.

## Preflight

Preflight connects to `Mezzanine.EvidenceLedger.Repo` and verifies owner
migration `20260416090000` in `schema_migrations`. Caller-supplied proof tokens
are not accepted as substrate evidence.

```elixir
Mezzanine.EvidenceLedger.Store.preflight()
Mezzanine.EvidenceLedger.Store.health()
```

Missing migration or database reachability fails before mutation. Raw secrets
and unbounded provider payload bodies remain forbidden from evidence state and
health output.

## Focused verification

```bash
cd core/evidence_engine
mix test test/mezzanine/evidence_ledger/store_test.exs \
  test/mezzanine/evidence_ledger/persistence_test.exs
```
