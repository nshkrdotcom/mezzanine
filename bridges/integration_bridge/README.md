# Mezzanine Integration Bridge

`bridges/integration_bridge` is the post-governance path from Mezzanine to the
public `jido_integration` platform facade.

It owns:

- direct run dispatch from `%Mezzanine.IntegrationBridge.AuthorizedInvocation{}`
- effect dispatch from `%Mezzanine.IntegrationBridge.AuthorizedInvocation{}`
- read dispatch via typed `Jido.Integration.V2.LowerFacts` operations keyed by
  authorized `ExecutionLineage`
- event translation back into Mezzanine audit attrs

`DirectRunDispatcher` and `EffectDispatcher` are post-Citadel only. They reject
generic maps, old `RunIntent` values, and old `EffectIntent` values through
function-head pattern matching before any provider effect can run. The raw
`Citadel.InvocationRequest.V2` struct or map representation must be carried in
the authorized invocation envelope with `AuthorityDecision.v1` and
`ExecutionGovernance.v1` evidence.

## Tenant-Scoped Lower Reads

`ReadDispatcher` is the Mezzanine substrate-facing lower-read boundary. It
builds a typed `Jido.Integration.V2.TenantScope` from the authorized read
intent and calls the dedicated Jido Integration substrate read slice through the
tenant-scoped `LowerFacts` facade.

Read intents that omit tenant scope or try to reuse lineage under another
tenant fail closed before the lower store is queried. Product code should not
call this bridge directly; northbound reads enter through AppKit surfaces and
carry Mezzanine read leases plus authorization scope.
