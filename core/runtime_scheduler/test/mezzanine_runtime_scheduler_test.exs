defmodule MezzanineRuntimeSchedulerTest do
  use ExUnit.Case

  test "exposes the reconcile-on-start component" do
    assert Mezzanine.RuntimeScheduler.ReconcileOnStart in MezzanineRuntimeScheduler.components()
  end
end
