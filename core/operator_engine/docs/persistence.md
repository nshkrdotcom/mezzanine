# Mezzanine Operator Engine Persistence

## Scope

Mezzanine Operator Engine owns operator command, diagnostics, control-room evidence, and operator store facade documentation for `core/operator_engine` in `mezzanine`.

## Available Tiers

- `:mickey_mouse`: memory or ref-only default. No restart durability claim.
- `:memory_debug`: memory or ref-only with redacted debug evidence only.
- `:local_restart_safe`: supported only when this package or a named adapter package owns a local durable store and preflight proof.
- `:integration_postgres`: supported only when a named Postgres or AshPostgres adapter and migration proof are configured.
- `:ops_durable`: supported only for Temporal-owning runtime packages after explicit substrate proof.
- `:full_debug_tracked`: supported only when durable storage and redacted debug capture are both explicitly preflighted.

## Default Tier

The default tier is `:mickey_mouse`. It is memory-only and does not prove restart durability.

## Capture Levels

Supported capture levels are `:off`, `:metadata`, `:refs_only`, and `:redacted_debug`. Raw credentials, auth headers, token files, credential bodies, raw prompt bodies, raw provider payload bodies, native auth file content, private keys, session cookies, refresh tokens, access tokens, database URLs with credentials, and object-store signed URLs are always forbidden.

## Supported Adapters

Memory store by default; AshPostgres descriptor only when explicitly selected.

## Unsupported Adapters

Unsupported adapter selections fail before mutation. Silent fallback from durable selection to memory is invalid.

## Configuration Precedence

Configuration is explicit caller data first, package option second, release profile third, and built-in default last. Governed flows do not read process environment, local credential files, provider defaults, singleton clients, or application configuration as authority unless this package names a standalone boot boundary.

## Example Config

```elixir
# Default deterministic profile.
[persistence_profile: :mickey_mouse]

# Durable opt-in example. The caller must also pass migration proof.
[persistence_profile: :integration_postgres, migration_proof: :present]
```

## Test Commands

```bash
cd core/operator_engine && mix test test/mezzanine/operator/store_test.exs
```

## Lost-On-Restart Claims

`:mickey_mouse` and `:memory_debug` data is lost on BEAM or process restart. Memory profiles may prove semantics, validation, and receipt shape; they do not prove restart durability.

## Valid Durability Claims

Valid durability claims require explicit profile selection, adapter capability, migration preflight, redacted evidence, focused tests, repo QC, and a pushed commit.

## Invalid Durability Claims

Invalid claims include ambient provider credentials, default database reachability, default Temporal reachability, object-store availability without opt-in, network reachability, raw debug capture, raw prompt capture, raw provider payload capture, and product direct lower-store imports.

## Debug Sidecar Behavior

Debug sidecars are disabled by default. When enabled, they are read-only or append-only redacted evidence surfaces. Debug failure must be non-mutating and must not alter operator, execution, workflow, store, projection, or product state.

## Redaction Guarantees

Evidence stores opaque refs, stable redacted ids, hashes, bounded metadata, claim-check refs, capture tags, receipt refs, store refs without credentials, and partition refs without secrets. Raw secret and raw payload fields are rejected before persistence or export.

## Migration And Preflight Behavior

Operator Postgres state uses the execution bounded-context repo; migration proof must be supplied before durable mutation.

## Phase 12 Migration And Preflight Closeout

- Tier: `:integration_postgres`.
- Schema owner: `Mezzanine.Execution.Repo` for operator-owned local mutation rows delegated to execution/object bounded contexts.
- Migration owner: `core/execution_engine/priv/repo/migrations` and `core/object_engine/priv/repo/migrations` for delegated durable operator effects.
- Migration preflight command: `Mezzanine.Operator.Store.preflight(profile: :integration_postgres, migration_proof: :present)`.
- Failure behavior: missing migration proof returns `{:error, {:missing_migration_proof, :operator}}` before operator mutation.
- Tagged test command: `cd core/operator_engine && mix test test/mezzanine/operator/store_test.exs`.
