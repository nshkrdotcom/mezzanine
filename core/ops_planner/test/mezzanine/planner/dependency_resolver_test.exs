defmodule Mezzanine.Planner.DependencyResolverTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Planner
  alias MezzanineOpsModel.WorkObject

  test "work with no dependency keys is ready to plan" do
    assert {:ok, work} =
             WorkObject.new(%{
               work_id: "work-ready",
               program_id: "program-1",
               work_type: "coding_task",
               title: "Ready work",
               payload: %{},
               status: :pending,
               dependency_keys: []
             })

    assert Planner.ready_to_plan?(work)
  end

  test "non-terminal dependency blocks planning" do
    refute Planner.DependencyResolver.blocked_by_dependencies?([
             %{id: "dep-1", status: :completed}
           ])

    assert Planner.DependencyResolver.blocked_by_dependencies?([
             %{id: "dep-2", status: :running}
           ])
  end
end
