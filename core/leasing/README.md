# Mezzanine Leasing

Durable leased direct-read and stream-attach substrate for Mezzanine.

This package owns:

- `read_leases`
- `stream_attach_leases`
- `lease_invalidations`

The active runtime path mints leases from governed upstream surfaces and treats
durable invalidation rows as the truth channel.

## Tenant Scope Enforcement

Read and stream leases are authorized through
`Mezzanine.Leasing.AuthorizationScope`. Callers must present the tenant,
installation, installation revision, activation epoch, lease epoch, subject,
execution, and trace scope they were authorized under when invoking
`authorize_read/5` or `authorize_stream_attach/4`.

The lease row remains the durable entitlement record, but the authorization
scope is the caller-carried proof that prevents a valid token from being reused
against another tenant, installation, installation revision, activation epoch,
lease epoch, subject, execution, or trace. Mismatches fail closed before token
digest, expiry, or invalidation checks can grant access.

## Revision And Revocation Evidence

Read and stream leases persist `installation_revision`, `activation_epoch`, and
`lease_epoch` as first-class fields. Lease invalidation rows carry the same
epoch evidence plus deterministic `revocation_ref` and `cache_invalidation_ref`
values. These rows back `Platform.InstallationRevisionEpoch.v1` and
`Platform.LeaseRevocation.v1` proof paths without making product code import
Mezzanine internals directly.
