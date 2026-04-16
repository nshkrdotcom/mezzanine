defmodule MezzaninePackCompiler do
  @moduledoc """
  Convenience entrypoint for the neutral Mezzanine pack compiler package.
  """

  defdelegate compile(pack_or_manifest), to: Mezzanine.Pack.Compiler
  defdelegate diagnostics(pack_or_manifest), to: Mezzanine.Pack.Compiler
end
