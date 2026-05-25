# Mezzanine Router Fabric

Mezzanine owns the stable adapter behaviours used by reusable routing and
optimization substrates. The concrete implementations live outside Mezzanine.

## Adapter Contracts

- `Mezzanine.AIExecution.RouterAdapter`
- `Mezzanine.AIExecution.OptimizerAdapter`

## Current Implementations

- `Trinity.MezzanineRouterAdapter` in
  `trinity_framework/core/trinity_coordinator_core`.
- `GEPA.MezzanineOptimizerAdapter` in `gepa_framework`.
- Mezzanine fixture adapters for deterministic tests and StackLab proofs.

## Boundary Rules

Router and optimizer adapters return refs, receipts, candidate summaries, and
failure reason codes. They do not mutate workflow truth, promote candidates, or
execute provider calls directly. Mezzanine remains the owner of workflow state,
idempotency, projections, and admission receipts.

## Local QC

```bash
mix ci
```
