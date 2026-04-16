# MezzanineRuntimeScheduler

Neutral runtime scheduler ownership for installation-scoped retry timing and
restart recovery.

This package now owns the first real Phase `2.7.3` runtime-scheduler slice:

- installation-scoped lease and fence ownership
- reconcile-on-start for dispatches stranded in `:dispatching`
- installation-scoped recovery summaries for restart orchestration
- durable replay handoff back into the execution-engine dispatcher

Primary modules:

- `MezzanineRuntimeScheduler`
- `Mezzanine.RuntimeScheduler.InstallationLeaseStore`
- `Mezzanine.RuntimeScheduler.ReconcileOnStart`
