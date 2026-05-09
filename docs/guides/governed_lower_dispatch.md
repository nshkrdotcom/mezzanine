# Governed Lower Dispatch

`Mezzanine.IntegrationBridge` is the public Mezzanine bridge into Jido
Integration after Citadel authority has been compiled.

## Entry Points

- `Mezzanine.IntegrationBridge.invoke_run_intent/2`
- `Mezzanine.IntegrationBridge.dispatch_effect/2`
- `Mezzanine.IntegrationBridge.dispatch_read/2`
- Linear helpers for source fetch, refresh, publication, and state updates
- GitHub helpers for PR creation, review, feedback, status, and cleanup

## Authorized Invocation

Lower dispatch requires `%Mezzanine.IntegrationBridge.AuthorizedInvocation{}`.
The invocation carries the Citadel invocation request plus tenant, subject,
execution, trace, and idempotency context.

The bridge builds a governed lower envelope before dispatch. Important envelope
fields include:

- `lower_request_ref`
- `lower_runtime_kind`
- `runtime_profile_ref`
- `capability_id` and `action_id`
- `connector_manifest_ref` and hash
- `capability_negotiation_ref`
- `policy_bundle_ref` and hash
- `cedar_schema_ref` and hash
- `script_ref` and hash
- `resource_scope_refs`
- `sandbox_profile_ref`
- `attestation_requirement_ref`
- `input_ref` and hash

## Dispatch Example

```elixir
{:ok, result} =
  Mezzanine.IntegrationBridge.invoke_run_intent(
    authorized_invocation,
    capability_id: "codex.session.turn",
    lower_runtime_kind: :codex_session,
    runtime_profile_ref: "runtime-profile://extravaganza/codex/default",
    resource_scope_refs: ["workspace://tenant/ENG-123"]
  )

result.governed_lower_receipt
```

## TRE Lane

TRE dispatch is intentionally explicit. A `:tre_rhai` lower runtime is allowed
only when the caller forwards a TRE adapter through invocation options. Without
that adapter, Mezzanine returns a governed lower denial before side effects.

## Fail-Closed Cases

The bridge rejects:

- old `RunIntent` values on lower dispatch paths
- generic map inputs where an authorized invocation is required
- unauthorized capabilities
- tenant or trace mismatches
- lower runtime kinds that are unavailable for the supplied invocation options
- lower reads without tenant-scoped execution lineage

## Receipt Shape

Success and failure both attach governed lower receipt data when a lower
dispatch result is available. Denials are represented as governed lower denial
records and should be projected as terminal governance evidence.
