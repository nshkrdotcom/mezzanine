# MezzaninePackCompiler

Pure validator, normalizer, compiler, and lifecycle evaluator for the neutral
Mezzanine pack runtime.

This package now owns:

- `Mezzanine.Pack.Compiler`
- structured validation diagnostics
- canonical identifier normalization
- `CompiledPack` O(1) index construction
- pure lifecycle transition evaluation against `SubjectSnapshot`
