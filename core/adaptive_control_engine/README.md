# Mezzanine Adaptive Control Engine

`mezzanine_adaptive_control_engine` coordinates closed-loop adaptation over
existing GEPA, TRINITY, trace, eval, replay, guardrail, cost, budget,
persistence, promotion, rollback, and AppKit refs.

It owns only the adaptive-control orchestration receipt. It does not implement
base memory, prompt, guardrail, eval, replay, cost, budget, persistence, model,
provider, GEPA framework, or TRINITY framework behavior.
