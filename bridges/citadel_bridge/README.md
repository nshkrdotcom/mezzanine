# Mezzanine Citadel Bridge

`bridges/citadel_bridge` lowers neutral Mezzanine run intents into Citadel's
pure substrate-governance surface.

It owns:

- `Mezzanine.Intent.RunIntent -> SubstrateGovernancePacket` assembly
- substrate trace, idempotency, installation revision, and tenant carriage
- invocation of `Citadel.Governance.SubstrateIngress`
- return of accepted lower invocation work or terminal governance rejection

It does not call `Citadel.HostIngress`, `SessionServer`, `SessionDirectory`, or
host continuity APIs. Host-origin sessions stay in Citadel host packages;
Mezzanine substrate-origin executions use the governance library directly.
