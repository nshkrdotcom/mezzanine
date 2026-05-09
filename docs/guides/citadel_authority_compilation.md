# Citadel Authority Compilation

Mezzanine uses `Mezzanine.CitadelBridge` to compile substrate-origin run
intents into Citadel governance output.

## Entry Points

- `Mezzanine.CitadelBridge.compile_run_intent/4`
- `Mezzanine.CitadelBridge.compile_submission/4`

## Inputs

The bridge expects a `Mezzanine.Intent.RunIntent`, run attributes, and selected
policy packs.

The intent and attributes should carry refs and bounded metadata:

- tenant id
- trace id
- idempotency key
- installation revision
- target id and kind
- selected capability or action id
- allowed operations
- workspace/resource refs
- policy refs
- runtime profile refs

## Example

```elixir
{:ok, compiled} =
  Mezzanine.CitadelBridge.compile_submission(
    run_intent,
    attrs,
    policy_packs,
    []
  )

compiled.invocation_request
compiled.authority_packet
compiled.execution_governance
```

## Boundary Rules

The bridge calls Citadel substrate governance. It does not call:

- `Citadel.HostIngress`
- host session servers
- session directories
- host continuity APIs

Host-origin sessions stay in Citadel host packages. Mezzanine substrate-origin
executions use Citadel governance directly.

## Output Contract

Accepted authority output should be converted into an authorized invocation for
lower dispatch. Rejection output should remain terminal governance evidence and
must not fall through into Jido Integration.
