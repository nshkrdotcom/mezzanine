# Mezzanine AI Run Model Persistence

AI run envelopes carry an explicit durable persistence reference. There is no
default profile and no memory fallback.

Supported profile identifiers are `:local_restart_safe`,
`:integration_postgres`, and `:ops_durable`; supported tiers are
`:postgres_shared` and `:temporal_postgres`. The NSHKR production profile is
`:ops_durable` on `:postgres_shared`.

`Mezzanine.AIRun.PersistenceRefs.production_profile/1` constructs the safe
production reference. `resolve/1` rejects an omitted profile, unavailable
durable substrate, `:mickey_mouse`, memory tiers, and malformed store,
partition, or retention refs.

Only refs and redacted metadata are retained. Raw credentials, prompts,
provider payloads, model output, tool bodies, memory bodies, and private
operator payloads are rejected by the envelope contract.
