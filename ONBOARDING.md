# mezzanine Onboarding

Read `AGENTS.md` first; Temporal work must use the repo-owned `just` commands.
`CLAUDE.md` must stay a one-line compatibility shim containing `@AGENTS.md`.

## Owns

Operational spine engines: lifecycle, workflow truth, projections, audit,
evidence, archival, barriers, operator state, and durable review/run truth.

## Does Not Own

Product UX, raw provider execution, raw semantic reasoning, connector SDK
mechanics, or universal primitives that belong in GroundPlane.

## First Task

```bash
cd /home/home/p/g/n/mezzanine
mix ci
cd /home/home/p/g/n/stack_lab
mix gn_ten.plan --repo mezzanine
```

## Proofs

StackLab owns assembled proof. Use `/home/home/p/g/n/stack_lab/proof_matrix.yml`
and `/home/home/p/g/n/stack_lab/docs/gn_ten_proof_matrix.md`.

## Common Changes

Use `just dev-up`, `just dev-status`, and related repo-owned commands for local
Temporal substrate work. Do not run ad hoc raw Temporal dev-server commands.
