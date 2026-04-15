# Mezzanine Execution Plane Bridge

`bridges/execution_plane_bridge` is the explicit future boundary for routing all
Mezzanine reads and effects through `execution_plane`.

For now it is intentionally honest: it returns typed not-supported errors until
the lower plane contract is ready for real integration.
