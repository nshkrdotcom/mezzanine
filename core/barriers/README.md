# Mezzanine Barriers

Durable fan-out and fan-in barrier ownership for `mezzanine`.

This package owns:

- `parallel_barriers`
- `parallel_barrier_completions`
- exact child-completion dedupe
- atomic barrier-close transitions

It does not own lifecycle advancement through an Oban join worker. Closed
barriers re-enter lifecycle through the Temporal fan-out/fan-in contract and
`Mezzanine.WorkflowRuntime` handoff path.
