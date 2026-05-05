# Mezzanine Headless Coding Ops

Phase 12 package for headless coding-ops intake, work items, session refs,
provider and target selection, operator controls, readback states, and
receipts.

The package is ref-only. It accepts authority refs and operator commands, and
it never accepts raw provider credentials, target credentials, local auth
files, or unmanaged runtime config.

QC:

```bash
mix test
mix format --check-formatted
mix compile --warnings-as-errors
```
