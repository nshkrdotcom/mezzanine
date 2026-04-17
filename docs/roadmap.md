# Roadmap

The repo is expected to grow into the neutral reusable home for:

- Ash-driven business semantics
- configurable workflow engines
- policy-rich operator logic
- reusable state models for distributed AI operations
- product-neutral orchestration machinery above `app_kit`

Current coexistence scaffold:

- neutral packages exist for the v3 rebuild
- legacy `ops_*` and `surfaces/program_surface` remain
  `[DEPRECATED-PENDING-MIGRATION]`
- lower bridges remain buildable while the neutral engines come online

Current gates:

- `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
- `MEZZANINE_NEUTRAL_CORE_CUTOVER`

Next slices:

- implement the neutral pack model and compiler
- implement the neutral durable engines on the new package graph
- retire the remaining deprecated `ops_*` northbound seams after each consumer cutover
