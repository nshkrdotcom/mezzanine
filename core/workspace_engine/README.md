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

Supported hook stages are `after_create`, `before_run`, `after_run`,
`before_remove`, plus legacy `prepare_workspace` and `after_turn`.
`after_create`, `before_run`, `before_remove`, and `prepare_workspace` are
fatal by default. `after_run` and `after_turn` are non-fatal by default and
return failed/timed-out receipts with `action: :continue`.

Hook receipts redact exact caller-supplied secret values and truncate string
outputs before they cross the workspace boundary. Public workspace projections
use `WorkspaceRecord.public_ref/1`; they expose opaque `workspace://...` refs
and safety metadata, never concrete local paths.

## Development

```bash
mix deps.get
mix ci
```
