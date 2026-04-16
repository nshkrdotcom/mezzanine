defmodule MezzaninePackCompilerTest do
  use ExUnit.Case

  alias Mezzanine.Pack.CompiledPack

  test "compile delegates to the neutral pack compiler" do
    assert {:ok, %CompiledPack{pack_slug: "expense_approval"}} =
             MezzaninePackCompiler.compile(Mezzanine.TestPacks.ExpenseApprovalPack)
  end
end
