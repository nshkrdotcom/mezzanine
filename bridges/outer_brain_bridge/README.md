# Mezzanine OuterBrain Bridge

Caller-owned transport contracts for Mezzanine context compile/readback calls
into OuterBrain.

The bridge provides direct, distributed, and fixture transports. Prompt
rendering is deliberately outside this transport: Mezzanine invokes
`OuterBrain.Prompting.ContextRenderer` through the AI execution runtime deps
after context compile/readback and stores the returned render result before
model invocation.

