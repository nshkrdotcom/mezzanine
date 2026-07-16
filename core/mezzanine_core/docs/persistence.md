# Mezzanine Core Persistence

Mezzanine run truth is Postgres-only in normal production composition.

- Repo: `Mezzanine.Repo`
- database selected by NSHKR: `nshkr_mezzanine`
- migration owner: `core/mezzanine_core/priv/repo/migrations`
- current required migration: `20260715100000`
- adapter: `Mezzanine.WorkflowRuntime.Store.Postgres`

One transaction inserts the idempotent command, run, first turn, first ordered
event, materialized projection, durable cursor, and pending workflow outbox.
An exact duplicate returns the original acceptance. Reuse of the same scoped
idempotency key with a different command or request hash is rejected.

Temporal is never called inside that transaction. The committed outbox is
claimed with `FOR UPDATE SKIP LOCKED` by the post-commit dispatcher.

Production preflight reaches the Repo and checks the actual schema migration.
Caller-supplied migration assertions are not accepted. Memory, fixture, no-op,
and static-success stores are not valid production selections.

Raw credentials and raw prompt/provider bodies do not belong in this schema;
the frozen command contract admits refs and digests only.
