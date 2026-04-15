# Mezzanine Ops Policy

Status: `[DEPRECATED-PENDING-MIGRATION]`

This package is part of the legacy `ops_*` ontology. It remains buildable only
during the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE`
and `MEZZANINE_NEUTRAL_CORE_CUTOVER`.

Pure policy loading and compilation for the Mezzanine workspace.

This package generalizes the original `symphony` `WORKFLOW.md` loader into a
reusable higher-order policy compiler. It is responsible for:

- loading policy bundles from strings, files, or maps
- parsing Markdown files with YAML front matter
- compiling typed run, approval, retry, placement, review, and grant settings
- preserving the last known good compiled policy on reload failures

It must stay free of:

- Ash
- OTP state
- connector/runtime calls
- product-specific business logic
