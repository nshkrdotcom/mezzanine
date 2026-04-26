# Mezzanine Workspace Engine

Neutral workspace allocation, lease, hook, cleanup, and path-safety contracts
for the generalized Symphony lane.

This package owns deterministic workspace refs and local path safety checks.
Lower process/SSH mechanics remain in Jido Integration and Execution Plane;
product-specific defaults remain in Extravaganza.

Workspace hooks are selected and timed out here, but their concrete
implementation is injected by the caller. Hook failures and hook timeouts fail
closed with typed receipts so workflow/runtime layers can record evidence and
stop before lower execution proceeds with a partially prepared workspace.

## Development

```bash
mix deps.get
mix ci
```
