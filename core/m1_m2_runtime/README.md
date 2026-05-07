# Mezzanine M1/M2 Runtime

Phase 12 package for deterministic M1 readback and live M2 runtime separation.

## Persistence Posture

M1 deterministic mode is memory-only. It rejects live provider, connector,
Temporal worker, credential materializer, Postgres, Temporal durable, object
store, local restart-safe, and other durable persistence selections. M2 remains
the mode that can carry explicit durable substrate refs.

M1 accepts fixture, readback, and projection facts only. It cannot call live
providers, live connectors, Temporal workers, or credential materializers.

M2 requires provider account, target attach, credential lease, operation
policy, runtime substrate refs, explicit `:ops_durable` Temporal persistence
profile, `:temporal_durable` capability, and substrate preflight proof before
live durable execution is admitted.

QC:

```bash
mix test
mix format --check-formatted
mix compile --warnings-as-errors
```

## Persistence Documentation

See `docs/persistence.md` for tiers, defaults, adapters, unsupported selections, config examples, restart claims, durability claims, debug sidecar behavior, redaction guarantees, migration or preflight behavior, and no-bypass scope when applicable.
