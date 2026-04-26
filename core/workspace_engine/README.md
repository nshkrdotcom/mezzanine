# Mezzanine Workspace Engine

Neutral workspace allocation, lease, hook, cleanup, and path-safety contracts
for the generalized Symphony lane.

This package owns deterministic workspace refs and local path safety checks.
Lower process/SSH mechanics remain in Jido Integration and Execution Plane;
product-specific defaults remain in Extravaganza.

## Development

```bash
mix deps.get
mix ci
```
