# Mezzanine Operator Engine Persistence

## Production posture

`Mezzanine.Operator.Store` selects `Store.AshPostgres` when options are omitted.
Production memory profiles are rejected; the deterministic memory adapter
exists only under `test/support` and is not compiled in production.

Operator mutations execute through `Mezzanine.OperatorCommands` and the
execution/object owner boundaries. No generic descriptor mutation path is
advertised.

## Preflight

Preflight connects to `Mezzanine.Execution.Repo` and verifies execution owner
migration `20260428114100` in `schema_migrations`. Caller-supplied proof tokens
are not accepted as substrate evidence.

```elixir
Mezzanine.Operator.Store.preflight()
Mezzanine.Operator.Store.health()
```

Missing migration or database reachability fails before operator mutation.

## Focused verification

```bash
cd core/operator_engine
mix test test/mezzanine/operator/store_test.exs \
  test/mezzanine/operator_commands_test.exs
```
