defmodule MezzanineCoreTest do
  use ExUnit.Case, async: true

  test "describes the initial reusable posture" do
    assert %{
             role: :business_semantics_substrate,
             posture: :configurable
           } = MezzanineCore.identity()
  end

  test "declares the first configuration axes" do
    assert :workflow in MezzanineCore.configuration_axes()
    assert :tenancy in MezzanineCore.configuration_axes()
  end

  test "exposes the frozen boundary generation posture" do
    assert [
             Mezzanine.Boundary.GenerationManifest,
             Mezzanine.Boundary.GenerationSpec
           ] == MezzanineCore.contract_modules()
  end
end
