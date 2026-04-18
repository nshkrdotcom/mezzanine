# Overview

Mezzanine is the intended reusable business-semantic layer for the nshkr stack.

It should absorb the high-level operational logic that is too product-specific
for lower platform layers yet too reusable to live inside any single proving
application.

The active Phase-2 rebuild in this repo now starts with the neutral package
graph:

- `pack_model`
- `pack_compiler`
- `lifecycle_engine`
- `config_registry`
- `object_engine`
- `execution_engine`
- `runtime_scheduler`
- `decision_engine`
- `evidence_engine`
- `projection_engine`
- `operator_engine`
- `audit_engine`
- `archival_engine`

The `ops_*` packages still host live semantic domains. New reusable work lands
in the neutral package graph while later phases migrate the remaining semantic
hosts to current package names and boundaries.
