# MezzaninePackCompiler

Pure validator, normalizer, compiler, and lifecycle evaluator for the neutral
Mezzanine pack runtime.

This package now owns:

- `Mezzanine.Pack.Compiler`
- structured validation diagnostics
- canonical identifier normalization
- source binding, source publish, workspace root, sandbox policy, prompt, hook,
  max-turn, and stall-timeout validation
- review decision, evidence policy, and operator action validation, including
  escalation decisions and pause/resume/retry/cancel action effects
- `CompiledPack` O(1) index construction
- pure lifecycle transition evaluation against `SubjectSnapshot`
