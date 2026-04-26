# Mezzanine Source Engine

Neutral source-event admission and dedupe contracts for the generalized
Symphony lane.

This package owns provider-neutral source facts such as `SourceBinding`,
`SourceCursor`, and `SourceEvent`. Provider API calls remain in Jido
Integration, and product-specific state names remain in Extravaganza.

Candidate admission is blocker-aware. The neutral classifier maps provider
state names through the installed `SourceBinding`, keeps non-terminal blocked
items in `candidate`, submits dispatchable unblocked items, and ignores
unrouted, unmapped, or terminal source states without making provider calls.

## Development

```bash
mix deps.get
mix ci
```
