# Mezzanine Headless Coding Ops

Phase 12 package for headless coding-ops intake, work items, session refs,
provider and target selection, operator controls, readback states, and
receipts.

The package is ref-only. It accepts authority refs and operator commands, and
it never accepts raw provider credentials, target credentials, local auth
files, or unmanaged runtime config.

Phase 14 handoff and resume proof uses `resume_handoff/1`. It preserves
tenant, session, provider account, connector binding, credential handle,
credential lease, native-auth assertion, target, attach grant, operation
policy, trace, idempotency, and active execution refs, and rejects raw
material or duplicate active execution before resuming.

QC:

```bash
mix test
mix format --check-formatted
mix compile --warnings-as-errors
```
