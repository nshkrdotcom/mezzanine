# MezzanineConfigRegistry

Neutral deployment and installation registry for the Mezzanine rebuild.

This package now owns the Phase `2.4.1` durable neutral registry slice:

- durable `PackRegistration` storage
- durable `Installation` storage
- compiled-pack payload persistence and revision metadata
- activation and suspension lifecycle state
- ETS-backed runtime cache keyed by installation revision
- internal/operator authoring bundle import and activation gates

Primary modules:

- `Mezzanine.Authoring.Bundle`
- `Mezzanine.Authoring.ExtensionPackBundle`
- `Mezzanine.Authoring.ExtensionPackSignature`
- `Mezzanine.ConfigRegistry.PackRegistration`
- `Mezzanine.ConfigRegistry.Installation`
- `Mezzanine.Pack.Serializer`
- `Mezzanine.Pack.Registry`

## Authoring Bundle Import

`MezzanineConfigRegistry.import_authoring_bundle/2` is the phase-3
internal/operator import path. It accepts a deterministic
`Mezzanine.Authoring.Bundle`, validates the bundle before persistence, and then
registers the pack plus creates or updates the installation in one transaction.

The pre-activation gates reject:

- invalid pack manifests, lifecycle spec echo, or decision spec echo
- missing execution bindings and missing required lifecycle hints
- policy refs outside the configured allowlist
- context adapter descriptors outside the trusted registry
- observer descriptors without valid subscriber metadata
- pack-authored platform migrations
- checksum mismatch
- signing/signature verification is not a v1 release claim unless Phase 1
  source-verifies signing modules and tests or Phase 7 implements signing
- stale installation revision before runtime reload

Bundle import does not load connector or context-adapter code and does not grant
pack authors any platform-table migration rights.

## Phase 4 Supply-Chain Evidence

`ExtensionPackSignature` and `ExtensionPackBundle` are Phase 4 formal evidence
contract candidates for `Platform.ExtensionPackSignature.v1` and
`Platform.ExtensionPackBundle.v1`. Until the owning executable proofs are green,
the v1 verified posture is checksum/schema validation. Signature verification
remains a post-v1/new-contract candidate unless Phase 1 source-verifies signing
modules and tests or Phase 7 implements signing. These contracts do not
introduce a second pack model or load connector code.
