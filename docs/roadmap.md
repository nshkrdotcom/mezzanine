# Roadmap

The repo is expected to grow into the neutral reusable home for:

- Ash-driven business semantics
- configurable workflow engines
- policy-rich operator logic
- reusable state models for distributed AI operations
- product-neutral orchestration machinery above `app_kit`

Current package posture:

- neutral packages carry new reusable substrate work
- `ops_*` packages remain as frozen semantic hosts for still-live consumers
- lower bridges remain buildable while the neutral engines come online

Next slices:

- implement the neutral pack model and compiler
- implement the neutral durable engines on the new package graph
- migrate the remaining `ops_*` semantic hosts into neutral packages with
  current names after each consumer cutover
