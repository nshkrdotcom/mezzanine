# AppKit And Product Boundary

Mezzanine is not the product-facing API. Product repositories should enter
through AppKit.

## Product Runtime Path

```text
Product repo
-> AppKit product-safe DTOs
-> Mezzanine backend services
-> Citadel / Jido / lower owners
-> Mezzanine projections
-> AppKit readback DTOs
-> Product UI or headless adapter
```

## Why AppKit Owns The Northbound Boundary

AppKit keeps product code from depending on Mezzanine internals. It also gives
multiple products a stable DTO surface while Mezzanine changes package layout
or internal persistence choices.

Product code should not import:

- `Mezzanine.CitadelBridge`
- `Mezzanine.IntegrationBridge`
- `Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow`
- Jido Integration modules
- Citadel modules
- ExecutionPlane modules
- provider SDKs

Product code may carry refs produced by these layers through AppKit DTOs.

## When Direct Mezzanine Use Is Acceptable

Direct Mezzanine use is acceptable for:

- AppKit bridge implementations
- Mezzanine package tests
- StackLab acceptance harnesses
- operator scripts that are explicitly below the product boundary

It is not acceptable for normal product runtime code unless the product is
itself an owner-package harness.
