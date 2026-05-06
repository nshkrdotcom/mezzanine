# Mezzanine AI Run Model

Ref-only AI run envelope, lifecycle, parent-child graph, and persistence
posture contracts for adaptive AI runs.

The package stores refs, lifecycle states, and safe summaries only. It rejects
raw prompts, provider payloads, model outputs, auth material, memory bodies,
tool bodies, and operator-private payloads.
