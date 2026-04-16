# MezzaninePackModel

Typed neutral domain-pack model for the Mezzanine rebuild.

This package owns:

- `Mezzanine.Pack` and the atomic `manifest/0` contract
- typed manifest and spec-family structs
- `Mezzanine.Pack.CompiledPack`
- validation issue types and pure lifecycle snapshot/context structs

It stays intentionally pure. Validation, normalization, compilation, and
lifecycle transition evaluation live in `core/pack_compiler`.
