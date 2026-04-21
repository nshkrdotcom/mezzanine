# Publication

Mezzanine is wired for Weld from day one.

The current publication target is the projected `mezzanine_core` artifact. The
workspace root remains tooling-only and should not become the runtime package.

Publication fidelity is checked by `mix artifact.fidelity.check` after
`mix weld.verify`. The check binds `build_support/weld.exs` source roots,
workspace and internal-modularity contracts, the generated
`dist/hex/mezzanine_core` projection, an artifact hash manifest, and the
manual-patch disposition so the projection cannot become a second source of
truth.
