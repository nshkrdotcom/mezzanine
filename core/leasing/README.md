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
installation, subject, execution, and trace scope they were authorized under
when invoking `authorize_read/5` or `authorize_stream_attach/4`.

The lease row remains the durable entitlement record, but the authorization
scope is the caller-carried proof that prevents a valid token from being reused
against another tenant, installation, subject, execution, or trace. Mismatches
fail closed before token digest, expiry, or invalidation checks can grant
access.
