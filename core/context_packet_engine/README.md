# Mezzanine Context Packet Engine

Durable, ref-only admission contracts for OuterBrain Context ABI packets.

This package admits already-compiled `OuterBrain.ContextABI.ContextPacket`
values after Citadel authority and Mezzanine budget checks. It stores packet
refs, hashes, authority refs, workflow refs, idempotency keys, and projection
joins only. It rejects raw prompts, raw memory bodies, provider payloads,
credentials, model outputs, private tool output, and lower-store payloads.

The package does not compile context, authorize context, render provider-native
messages, call models, or expose product DTOs. OuterBrain owns compilation and
rendering. Citadel owns authority. Jido Integration owns model invocation.
