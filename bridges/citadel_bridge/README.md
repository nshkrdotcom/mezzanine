# Mezzanine Citadel Bridge

`bridges/citadel_bridge` lowers neutral Mezzanine run intents into Citadel's
pure substrate-governance surface.

It owns:

- `Mezzanine.Intent.RunIntent -> SubstrateGovernancePacket` assembly
- substrate trace, idempotency, installation revision, and tenant carriage
- grant-profile allowed tools from the neutral run intent, so product and pack
  policy reach Citadel governance without env-backed process configuration
- invocation of `Citadel.Governance.SubstrateIngress`
- return of accepted lower invocation work or terminal governance rejection

It does not call `Citadel.HostIngress`, `SessionServer`, `SessionDirectory`, or
host continuity APIs. Host-origin sessions stay in Citadel host packages;
Mezzanine substrate-origin executions use the governance library directly.

Coding-ops runs should pass the selected Citadel coding-ops policy pack to this
bridge. Citadel then owns sandbox, egress, approval, allowed-tool,
allowed-operation, workspace-mutability, and placement enforcement before any
Jido lower submission exists.
