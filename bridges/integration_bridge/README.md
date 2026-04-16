# Mezzanine Integration Bridge

`bridges/integration_bridge` is the narrow direct path from Mezzanine intents to
the public `jido_integration` platform facade for cases that do not need full
Brain routing.

It owns:

- direct run dispatch
- effect dispatch via connector-backed capabilities
- read dispatch via typed `Jido.Integration.V2.LowerFacts` operations keyed by
  authorized `ExecutionLineage`
- event translation back into Mezzanine audit attrs
