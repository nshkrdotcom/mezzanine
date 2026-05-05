# Mezzanine M1/M2 Runtime

Phase 12 package for deterministic M1 readback and live M2 runtime separation.

M1 accepts fixture, readback, and projection facts only. It cannot call live
providers, live connectors, Temporal workers, or credential materializers.

M2 requires provider account, target attach, credential lease, operation
policy, and runtime substrate refs before live durable execution is admitted.

QC:

```bash
mix test
mix format --check-formatted
mix compile --warnings-as-errors
```
