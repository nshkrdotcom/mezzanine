# Mezzanine Barriers

Durable fan-out and fan-in barrier ownership for `mezzanine`.

This package owns:

- `parallel_barriers`
- `parallel_barrier_completions`
- exact child-completion dedupe
- atomic barrier-close transitions

It does not own lifecycle advancement directly. Closed barriers re-enter
`LifecycleEvaluator` through `JoinAdvanceWorker` on the lifecycle side of the
package graph.
