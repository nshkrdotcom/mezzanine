# Mezzanine Context ABI Admission

Mezzanine owns workflow truth and AI execution admission. It admits
OuterBrain-owned Context ABI packets, joins them to Citadel authority, and
holds the rendered prompt handoff before dispatching model invocation through
Jido Integration.

## Owned Modules

- `Mezzanine.ContextPacketEngine.Admitter`
- `Mezzanine.ContextPacketEngine.AdmissionRequest`
- `Mezzanine.ContextPacketEngine.AdmissionReceipt`
- `Mezzanine.AIExecution.RenderResult`
- `Mezzanine.AIExecution.RuntimeDeps`

## Runtime Handoff

1. Mezzanine requests Citadel authority.
2. Mezzanine admits an OuterBrain `ContextPacket`.
3. Mezzanine invokes `OuterBrain.Prompting.ContextRenderer` through
   `Mezzanine.AIExecution.RuntimeDeps.renderer`.
4. Mezzanine stores the returned refs as `Mezzanine.AIExecution.RenderResult`.
5. Mezzanine passes `prompt_artifact_ref`, `provider_payload_ref`, and
   `payload_hash` into the Jido Integration model invocation request.

Mezzanine does not inspect raw prompt bodies or provider-native payloads.

## Local QC

```bash
mix ci
```
