# Mezzanine Leasing

Durable leased direct-read and stream-attach substrate for Mezzanine.

This package owns:

- `read_leases`
- `stream_attach_leases`
- `lease_invalidations`

The active runtime path mints leases from governed upstream surfaces and treats
durable invalidation rows as the truth channel.
