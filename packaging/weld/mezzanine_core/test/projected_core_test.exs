defmodule MezzanineCoreProjectedTest do
  use ExUnit.Case, async: true

  test "projects the reusable configuration axes" do
    assert :workflow in MezzanineCore.configuration_axes()
  end
end
