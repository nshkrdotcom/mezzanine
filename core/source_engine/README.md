# Mezzanine Source Engine

Neutral source-event admission and dedupe contracts for the generalized
Symphony lane.

This package owns provider-neutral source facts such as `SourceBinding`,
`SourceCursor`, and `SourceEvent`. Provider API calls remain in Jido
Integration, and product-specific state names remain in Extravaganza.

## Development

```bash
mix deps.get
mix ci
```
