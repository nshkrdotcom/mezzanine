# Mezzanine Workspace Build Model

Phase 12 package for workspace descriptors, build manifests, plugin
boundaries, and no-secret runtime invocation refs.

The package produces build manifests for agent names, triggers, roles,
required providers, connector bindings, target postures, environment contract
refs, secret contract refs, and projection refs. It rejects raw auth values,
local token files, private path state, and unmanaged workspace auth material.

QC:

```bash
mix test
mix format --check-formatted
mix compile --warnings-as-errors
```
