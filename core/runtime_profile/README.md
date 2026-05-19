# Mezzanine Runtime Profile

Boot-loaded runtime configuration profile and supervised profile owner.

This package owns the neutral runtime profile used by Mezzanine packages that
need configurable module or keyword choices at runtime without reading
application environment deep inside call stacks. Application env is captured at
application boot into a `Mezzanine.RuntimeProfile` value, and runtime code reads
that value through the supervised `Mezzanine.RuntimeProfileStore`.
