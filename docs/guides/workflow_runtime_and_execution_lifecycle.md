# Workflow Runtime And Execution Lifecycle

Mezzanine workflow runtime is split into a public facade and deterministic
workflow contract modules.

## Runtime Facade

`Mezzanine.WorkflowRuntime` is the only public Mezzanine Temporal client
boundary.

Functions:

- `start_workflow/1`
- `signal_workflow/1`
- `query_workflow/1`
- `cancel_workflow/1`
- `describe_workflow/1`
- `fetch_workflow_history_ref/1`

The default implementation is `Mezzanine.WorkflowRuntime.Unconfigured`, which
fails closed. Temporal-enabled deployments configure:

```elixir
config :mezzanine_core,
  workflow_runtime_impl: Mezzanine.WorkflowRuntime.TemporalexAdapter
```

## Execution Lifecycle Workflow

`Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow` defines the governed
execution-attempt lifecycle:

1. Build normalized workflow input.
2. Compile Citadel authority.
3. Submit Jido lower run.
4. Accept or wait for lower receipt signal.
5. Persist terminal receipt.
6. Cleanup workspace.
7. Publish source updates when requested.
8. Update runtime projection.
9. Materialize evidence.
10. Create review.

## Activity Surfaces

The lifecycle exposes activity-shaped functions for tests and adapters:

- `compile_citadel_authority_activity/1`
- `submit_jido_lower_run_activity/1`
- `persist_terminal_receipt_activity/1`
- `cleanup_workspace_activity/1`
- `publish_source_activity/1`
- `update_runtime_projection_activity/1`
- `materialize_evidence_activity/1`
- `create_review_activity/1`

## Signal And Query Surfaces

Receipt and operator state surfaces include:

- `receipt_signal/1`
- `deliver_receipt_signal/1`
- `apply_receipt_signal/2`
- `terminal_receipt_policy/2`
- `query_operator_state/1`

## Temporal Development

Temporal development is repo-owned:

```bash
cd /home/home/p/g/n/mezzanine
just dev-up
just dev-status
just dev-logs
just temporal-ui
```

Do not start ad hoc Temporal processes for Mezzanine development.
