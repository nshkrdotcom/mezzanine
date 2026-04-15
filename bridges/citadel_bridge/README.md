# Mezzanine Citadel Bridge

`bridges/citadel_bridge` lowers Mezzanine semantic run intents into Citadel's
public structured host-ingress surface.

It owns:

- `RunIntent -> Citadel.HostIngress.RunRequest` compilation
- host-ingress request-context assembly
- placement binding for the public Citadel ingress contract
- lower-event mapping back into Mezzanine audit attrs
