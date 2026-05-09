# Runtime Stack Overview

Mezzanine sits between product-safe AppKit DTOs and lower infrastructure
owners. It is not a product repository and it is not a lower runtime.

## Owner Map

- Product repo: owns product language, templates, profiles, and UX.
- AppKit: owns product-safe DTOs and northbound app surfaces.
- Mezzanine: owns reusable work/run/review/runtime/projection semantics.
- Citadel: owns authority compilation and governance decisions.
- Jido Integration: owns connector and lower invocation routing.
- ExecutionPlane: owns lower runtime execution lanes.
- StackLab: owns acceptance harnesses and deployment checks.

## Runtime Shape

```text
product request
-> AppKit surface
-> Mezzanine.WorkControl
-> Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow
-> Mezzanine.CitadelBridge
-> Mezzanine.IntegrationBridge
-> Jido.Integration.V2
-> lower runtime or connector
-> governed lower receipt
-> Mezzanine.Projections.ReceiptReducer
-> AppKit readback DTO
```

## Boundary Rules

- Product runtime code should not import Citadel, Jido Integration,
  ExecutionPlane, provider SDKs, or lower store modules.
- Mezzanine bridge code should not import product internals.
- Citadel decisions must be represented as refs, hashes, policy refs, and
  governance metadata before lower dispatch.
- Lower effects must produce governed receipts or governed denials.
- Readback should be projection-driven and ref-oriented.

## What Mezzanine Does Not Own

Mezzanine does not own:

- live provider credentials
- raw provider payload storage
- direct Linear/GitHub/Codex SDK calls outside Jido Integration
- Phoenix UI logic
- product-specific copy or UX
- TRE execution implementation

It can carry refs, routing facts, evidence summaries, and terminal receipt
state for all of those lower or product concerns.
