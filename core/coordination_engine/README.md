# Mezzanine Coordination Engine

Governed TRINITY coordination orchestration for router readiness, provider
pool readiness, role injection, bounded turns, verifier policy, handoff,
cancellation, replay, and operator-visible state.

This package stores refs, lifecycle states, safe decisions, and receipts only.
It does not carry raw prompts, provider payloads, model outputs, memory bodies,
message bodies, tool bodies, secrets, or workflow histories.

## Trace Dataset Projection

`Mezzanine.CoordinationEngine.trace_dataset/2` projects governed coordination
run traces into eval and replay dataset refs for prior-fabric integration. The
projection carries role prompt refs, memory refs, context-budget refs,
guardrail refs, eval refs, replay refs, cost refs, trace refs, verifier refs,
AppKit projection refs, AITrace span refs, store-tier refs, retention refs, and
local restart-safe persistence posture refs.

Dataset receipts are refs-only and reject raw prompt, provider, model, memory,
message, tool, credential, secret, or workflow payloads. This keeps TRINITY
coordination traces consumable by downstream adaptive-control phases without
introducing base memory, prompt, guardrail, eval, replay, cost, persistence, or
provider SDK ownership into this package.
