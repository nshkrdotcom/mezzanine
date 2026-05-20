# Mezzanine Governed Effects

Pure governed-effect contract structs for Mezzanine.

This package owns Mezzanine's internal lifecycle view of governed effects:

- `Mezzanine.Core.GovernedEffects.GovernedEffect`
- `Mezzanine.Core.GovernedEffects.EffectReceipt`
- `Mezzanine.Core.GovernedEffects.AuthorityPacket`

`GroundPlane.BoundaryProtocol.CommandEnvelope` remains the canonical boundary
command envelope. These Mezzanine structs carry the same `tenant_ref`,
`actor_ref`, `command_ref`, and `trace_ref` vocabulary so later phases can
compose them without field-name translation inside Mezzanine.

The structs are internal lifecycle objects, not external GAOP JSON schemas.
They serialize through `GroundPlane.Boundary.Codec` and expose status and
decision atoms in Elixir while dumping those atoms as strings for canonical
boundary JSON.
