# Mezzanine QC And Operations

## Local Commands

```bash
mix deps.get
mix ci
```

Temporal development is repo-owned:

```bash
just dev-up
just dev-status
just dev-logs
just temporal-ui
```

Use package-local tests for focused changes, then root `mix ci` before commit.

## Scanner And Proof Obligations

Mezzanine changes must keep these obligations green:

- binding registry and ConfigRegistry tests for activation, epoch invalidation,
  stale-cache fail-closed behavior, and run snapshot retention;
- lifecycle/workflow/runtime tests for dispatch, retry, cancellation, recovery,
  and projection reduction;
- StackLab tenant, connector, restart-fencing, and proof-matrix checks where
  the change crosses repo boundaries;
- no Regex usage in touched code/tests;
- no dynamic atom construction from runtime input;
- every process, worker, hook runner, scheduler, and listener is supervised.

## Secrets And Live Providers

Mezzanine stores credential lease refs and binding data, not raw provider
credentials. Live GitHub or Linear commands are invoked through product or
lower-adapter acceptance paths and must be prefixed with:

```bash
~/scripts/with_bash_secrets
```

## Tenant, Observability, And Replay

Mezzanine rows and receipts must carry tenant, installation, binding epoch,
authority, operation, lower receipt, and trace refs. AITrace receives emitted
execution events; Mezzanine is responsible for emitting enough structured
events for replay to reconstruct lifecycle decisions without raw provider
payloads.

## Documentation Checks

After doc edits, run:

```bash
test -f README.md
find guides -maxdepth 1 -type f -name '*.md' -print | sort
git diff --check -- README.md guides
```
