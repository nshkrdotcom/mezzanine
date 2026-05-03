defmodule Mix.Tasks.Pack.LintTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Pack.Lint, as: PackLint

  setup do
    Mix.Task.reenable("pack.lint")
    :ok
  end

  test "reports success for a valid pack module" do
    output =
      capture_io(fn ->
        PackLint.run(["Mezzanine.TestPacks.ExpenseApprovalPack"])
      end)

    assert output =~ "pack lint passed"
  end

  test "raises when the requested pack module is missing" do
    error =
      assert_raise Mix.Error, fn ->
        capture_io(fn ->
          PackLint.run(["Mezzanine.TestPacks.DoesNotExist"])
        end)
      end

    assert String.contains?(Exception.message(error), "pack lint failed")
  end
end
