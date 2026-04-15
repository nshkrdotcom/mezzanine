# Mezzanine AppKit Bridge

Status: `[DEPRECATED-PENDING-MIGRATION]`

This is the frozen legacy northbound bridge. It remains buildable only during
the coexistence window guarded by `NO_NEW_PRODUCT_DEP_ON_OLD_MEZZANINE` and
`MEZZANINE_NEUTRAL_CORE_CUTOVER`, and no new surface area should be added here.

`bridges/app_kit_bridge` adapts Mezzanine's durable work, planning, audit, and
review services to the existing `app_kit` backend contracts.

It stays northbound-only:

- `AppKit.WorkControl` can use `Mezzanine.AppKitBridge.WorkControlAdapter`
- `AppKit.OperatorSurface` can use
  `Mezzanine.AppKitBridge.OperatorProjectionAdapter`

This package does not make `app_kit` depend on `mezzanine`; it only provides
the optional backend implementation on the Mezzanine side.
