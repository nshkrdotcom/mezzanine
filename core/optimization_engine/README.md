# Mezzanine Optimization Engine

Governed GEPA orchestration for evaluation batches, objective registry,
candidate lifecycle, budget refs, checkpoint refs, and promotion decisions.

This package stores refs, decisions, summaries, and receipts only. It does not
carry raw prompts, provider payloads, model outputs, memory bodies, secrets, or
workflow histories.

## Prior Fabric Binding

`Mezzanine.OptimizationEngine.bind_prior_fabric/1` binds inherited memory,
prompt, context-budget, guardrail, eval, replay, cost-budget, drift, trace,
persistence, promotion, and rollback refs into a GEPA run receipt. The adapter
validates that every inherited surface is represented by refs, that store-tier
and local restart-safe persistence posture refs are explicit, and that raw
prompt, provider, model, memory, credential, secret, or workflow payloads are
absent.

Promotion decisions require operator-visible evidence refs for eval, replay,
guardrail, and budget gates before a candidate can be promoted. Missing gate
evidence or missing operator evidence leaves the decision blocked with rollback
refs instead of silently accepting a promotion.
