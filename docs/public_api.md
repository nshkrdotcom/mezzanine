# Public Elixir API Surface

This document names the public Elixir entrypoints that product and platform
callers may depend on. Everything else is package-internal unless a package
README or guide in this repo explicitly promotes it.

## Boundary Rule

Product repositories should not call Mezzanine directly in normal product
runtime paths. Product code enters through AppKit. AppKit owns the northbound
DTO boundary and delegates to Mezzanine where the selected backend is
Mezzanine-backed.

Mezzanine owns reusable business and runtime semantics. It must not reach into
product internals, provider SDKs, or lower stores outside the named bridges and
facades below.

## Supported Public Surfaces

### Work Control

Module: `Mezzanine.WorkControl`

Use this surface to prepare and start governed work from a product-safe subject
or work object once AppKit has admitted the request.

Primary functions:

- `prepare_run_request/2`
- `start_run_for_subject/3`
- `control_session_for_work/2`
- `ensure_control_session/2`

This surface creates and updates Mezzanine-owned work, run, control-session,
and review records. It does not perform provider effects.

### Citadel Authority Bridge

Module: `Mezzanine.CitadelBridge`

Use this surface to compile a Mezzanine `RunIntent` into Citadel substrate
governance.

Primary functions:

- `compile_run_intent/4`
- `compile_submission/4`

This surface is substrate-origin only. It calls Citadel governance packages,
not Citadel host-session APIs.

### Governed Lower Dispatch

Module: `Mezzanine.IntegrationBridge`

Use this surface after Citadel has produced authority evidence and the caller
has a `%Mezzanine.IntegrationBridge.AuthorizedInvocation{}`.

Primary functions:

- `invoke_run_intent/2`
- `dispatch_effect/2`
- `dispatch_read/2`
- `fetch_source_candidates/4`
- `refresh_source_item/5`
- `fetch_source_current_states/5`
- `normalize_source_page/5`
- `source_read_allowed_operations/3`
- `publish_source/5`
- `source_publication_allowed_operations/4`
- `invoke_runtime_operation/6`
- `runtime_operation_allowed_operations/5`
- `invoke_runtime_tool/6`
- `runtime_tool_allowed_operations/5`
- `collect_evidence/4`
- `evidence_allowed_operations/4`
- `invoke_resource_effect/4`
- `resource_effect_allowed_operations/4`
- `to_audit_attrs/2`

The bridge rejects old intent structs and generic maps for lower effects before
execution. Generic dispatch functions require role refs plus binding data; any
provider-specific lower work is resolved from binding data into explicit
provider-adapter or connector zones. Lower execution is delegated to
`Jido.Integration.V2`.

### Workflow Runtime

Module: `Mezzanine.WorkflowRuntime`

Use this facade for Temporal start, signal, query, cancel, describe, and
history-reference operations. The default implementation is unconfigured and
fails closed. Deployments opt into a concrete implementation, normally
`Mezzanine.WorkflowRuntime.TemporalexAdapter`.

Primary functions:

- `start_workflow/1`
- `signal_workflow/1`
- `query_workflow/1`
- `cancel_workflow/1`
- `describe_workflow/1`
- `fetch_workflow_history_ref/1`

This surface returns Mezzanine DTOs and does not expose Temporal SDK structs,
task tokens, NIF resources, protobufs, or raw workflow history.

### Agent Turn Engine

Module: `Mezzanine.AgentTurnEngine`

Use this package-local surface to validate and reduce native agent turn facts:
ledgers, conversation events, execution events, replay requests, cursor
catch-up state, and pending interactions. This surface is pure and does not
start processes, query stores, call providers, or expose generated protocol
modules.

Primary modules:

- `Mezzanine.AgentTurnEngine.AgentTurnLedger`
- `Mezzanine.AgentTurnEngine.AgentConversationEvent`
- `Mezzanine.AgentTurnEngine.AgentExecutionEvent`
- `Mezzanine.AgentTurnEngine.AgentRunCursor`
- `Mezzanine.AgentTurnEngine.ExecutionReplay`
- `Mezzanine.AgentTurnEngine.AgentPendingInteraction`
- `Mezzanine.AgentTurnEngine.Reducer`

Products still enter through AppKit. Store adapters, workflow integration, and
AITrace export are separate Mezzanine phases layered on top of these contracts.

### Context Packet Admission

Module: `Mezzanine.ContextPacketEngine`

Use this surface to admit an `OuterBrain.ContextABI.ContextPacket` after
Citadel authority and Mezzanine budget gates have been evaluated.

Primary functions:

- `admit/3`
- `redacted_projection/1`

This surface returns Mezzanine packet-admission receipts and ref-only projection
source data. It does not compile context, authorize context, render prompts, or
call providers.

### AI Execution Contracts

Module: `Mezzanine.AIExecution`

Use this surface for route/optimization adapter invocation and the
rendered-prompt handoff from OuterBrain refs into model-invocation requests.

Primary functions:

- `route/3`
- `propose/3`
- `render_context/4`
- `invocation_request/3`

The concrete TRINITY and GEPA integrations implement
`Mezzanine.AIExecution.RouterAdapter` and
`Mezzanine.AIExecution.OptimizerAdapter`. This surface does not execute model
providers directly.

### Execution Lifecycle Workflow Contract

Module: `Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow`

Use this surface for the deterministic execution-attempt lifecycle contract and
activity boundary tests.

Primary functions include:

- `contract/0`
- `new_input/1`
- `run/1`
- `runtime_result/3`
- `initial_state/1`
- `compile_citadel_authority_activity/1`
- `submit_jido_lower_run_activity/1`
- `persist_terminal_receipt_activity/1`
- `update_runtime_projection_activity/1`
- `cleanup_workspace_activity/1`
- `publish_source_activity/1`
- `materialize_evidence_activity/1`
- `create_review_activity/1`
- `receipt_signal/1`
- `deliver_receipt_signal/1`
- `apply_receipt_signal/2`
- `terminal_receipt_policy/2`
- `query_operator_state/1`
- `execution_control_policy/1`
- `turn_loop_decision/1`
- `worker_failover_recovery/1`
- `incident_fields/1`

This is the canonical Mezzanine lifecycle shape for a governed lower run.

### Receipt Reduction And Readback

Module: `Mezzanine.Projections.ReceiptReducer`

Use this surface to reduce terminal lower receipts into Mezzanine-owned
execution, subject, decision, evidence, projection, and audit ledgers.

Primary function:

- `reduce/1`

The reducer accepts refs and structured receipt metadata. It does not discover
provider objects through static selectors and it does not read process
environment.

## Internal Or Experimental Surfaces

These modules may be useful in tests or owner-package internals, but they are
not product-facing public APIs unless a guide explicitly routes through them:

- Ash resource modules and repos
- persistence store adapter internals
- Temporal worker modules and activity wrappers
- package-local scanner/proof modules
- bridge implementation modules behind `Mezzanine.IntegrationBridge`
- `Mezzanine.WorkflowRuntime.TemporalexBoundary`

## Compatibility Notes

- Public surfaces are ref-oriented and DTO-oriented.
- Raw provider payloads, raw workflow history, credentials, token files,
  Temporal SDK structs, task tokens, and direct lower store selectors are not
  public data contracts.
- For local end-to-end acceptance of the governed lower lane, use StackLab.
  StackLab is an external proof harness, not a Mezzanine runtime package.
