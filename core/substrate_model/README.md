# Mezzanine Substrate Model

Pure generic operational substrate structs and reducers.

This package owns data and pure functions only. It has no database, process,
connector, authority, AppKit, Jido, Citadel, Execution Plane, product, or bridge
dependency. Boundary packages build on these structs after they have resolved
their own effects and authority.
