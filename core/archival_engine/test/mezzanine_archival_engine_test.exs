defmodule MezzanineArchivalEngineTest do
  use ExUnit.Case

  test "lists the frozen archival contract modules" do
    assert [
             Mezzanine.Archival.CountdownPolicy,
             Mezzanine.Archival.Graph,
             Mezzanine.Archival.Manifest,
             Mezzanine.Archival.OffloadPlan
           ] == MezzanineArchivalEngine.contract_modules()
  end
end
