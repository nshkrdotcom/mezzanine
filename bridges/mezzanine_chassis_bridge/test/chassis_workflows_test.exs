ExUnit.start()

defmodule Mezzanine.ChassisWorkflowsTest do
  use ExUnit.Case, async: true

  test "deployment workflow dispatches" do
    assert {:ok, %{workflow: :chassis_deployment}} =
             Mezzanine.Workflow.ChassisDeploymentWorkflow.dispatch()
  end
end
