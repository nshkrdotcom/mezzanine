# MezzanineRuntimeScheduler

Neutral runtime scheduler ownership for installation-scoped retry timing and
restart recovery.

This package now owns the first real Phase `2.7.3` runtime-scheduler slice:

- installation-scoped lease and fence ownership
- stale compiled-pack revision rejection with attempted/current revision and
  fencing diagnostics
- reconcile-on-start for dispatches stranded in `:dispatching`
- startup reconcile enqueue for executions stranded in `:awaiting_receipt`
- installation-scoped recovery summaries for restart orchestration across both
  dispatch and accepted-without-receipt recovery
- durable replay handoff back into the execution-engine dispatch worker

Primary modules:

- `MezzanineRuntimeScheduler`
- `Mezzanine.RuntimeScheduler.InstallationLeaseStore`
- `Mezzanine.RuntimeScheduler.ReconcileOnStart`
