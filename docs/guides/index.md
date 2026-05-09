# Guides

These guides explain how to consume Mezzanine's public Elixir API surfaces
without bypassing AppKit, Citadel, Jido Integration, or ExecutionPlane
ownership.

Read them in this order:

1. [Runtime Stack Overview](runtime_stack_overview.md)
2. [Work Control Run Lifecycle](work_control_run_lifecycle.md)
3. [Citadel Authority Compilation](citadel_authority_compilation.md)
4. [Governed Lower Dispatch](governed_lower_dispatch.md)
5. [Workflow Runtime And Execution Lifecycle](workflow_runtime_and_execution_lifecycle.md)
6. [Receipts And Projections](receipts_and_projections.md)
7. [AppKit And Product Boundary](appkit_and_product_boundary.md)
8. [Local Acceptance With StackLab](local_acceptance_with_stacklab.md)

The shortest useful path for a governed lower run is:

```text
AppKit
-> Mezzanine.WorkControl
-> Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow
-> Mezzanine.CitadelBridge
-> Mezzanine.IntegrationBridge
-> Jido.Integration.V2
-> lower owner runtime
-> Mezzanine.Projections.ReceiptReducer
```

The product-facing API should remain AppKit-owned. Mezzanine is the reusable
semantic and runtime owner behind that boundary.
