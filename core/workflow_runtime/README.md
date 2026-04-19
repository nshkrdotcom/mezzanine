# Mezzanine Workflow Runtime

Owns the Phase 4 Temporal runtime boundary for Mezzanine.

This package is the only Mezzanine package that compiles the direct
`temporalex` runtime dependency. Core substrate packages keep pure contract and
ledger ownership; workflow execution code stays isolated here so monorepo
quality checks do not compile the Temporal Rust/NIF bridge transitively through
every engine.
