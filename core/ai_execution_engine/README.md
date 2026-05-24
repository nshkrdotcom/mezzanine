# Mezzanine AI Execution Engine

Generalized AI execution contracts for routing, optimization, prompt-render
handoff, and model-invocation request assembly.

This package owns Mezzanine adapter behaviours for TRINITY routing and GEPA
optimization. It also normalizes the OuterBrain prompt-rendering return value
into `Mezzanine.AIExecution.RenderResult`, which is the explicit handoff into
Jido Integration model invocation.

The package stores refs, hashes, decisions, and safe receipts only. It does not
compile context packets, authorize context, execute providers, hold
credentials, or persist raw prompts/provider payloads/model outputs.
