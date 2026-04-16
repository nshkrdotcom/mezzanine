# MezzanineConfigRegistry

Neutral deployment and installation registry for the Mezzanine rebuild.

This package now owns the Phase `2.4.1` durable neutral registry slice:

- durable `PackRegistration` storage
- durable `Installation` storage
- compiled-pack payload persistence and revision metadata
- activation and suspension lifecycle state
- ETS-backed runtime cache keyed by installation revision

Primary modules:

- `Mezzanine.ConfigRegistry.PackRegistration`
- `Mezzanine.ConfigRegistry.Installation`
- `Mezzanine.Pack.Serializer`
- `Mezzanine.Pack.Registry`
