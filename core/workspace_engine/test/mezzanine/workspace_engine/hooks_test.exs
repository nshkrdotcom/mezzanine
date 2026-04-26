defmodule Mezzanine.WorkspaceEngine.HooksTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkspaceEngine.{Allocator, Hooks}

  test "runs matching workspace hooks and returns receipts" do
    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: tmp_dir(),
        hook_specs: [
          %{"hook_ref" => "prepare", "stage" => "prepare_workspace", "timeout_ms" => 100}
        ]
      })

    assert {:ok, [receipt]} =
             Hooks.run(workspace, :prepare_workspace,
               runner: fn hook, context ->
                 assert hook.hook_ref == "prepare"
                 assert context.workspace_id == workspace.workspace_id
                 {:ok, %{prepared?: true}}
               end
             )

    assert receipt.hook_ref == "prepare"
    assert receipt.status == :succeeded
    assert receipt.result == %{prepared?: true}
  end

  test "fails closed when a workspace hook fails" do
    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: tmp_dir(),
        hook_specs: [
          %{"hook_ref" => "prepare", "stage" => "prepare_workspace", "timeout_ms" => 100}
        ]
      })

    assert {:error, {:hook_failed, receipt}} =
             Hooks.run(workspace, :prepare_workspace,
               runner: fn _hook, _context -> {:error, :missing_dependency} end
             )

    assert receipt.hook_ref == "prepare"
    assert receipt.status == :failed
    assert receipt.reason == :missing_dependency
  end

  test "fails closed when a workspace hook times out" do
    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: tmp_dir(),
        hook_specs: [
          %{"hook_ref" => "prepare", "stage" => "prepare_workspace", "timeout_ms" => 1}
        ]
      })

    assert {:error, {:hook_timeout, receipt}} =
             Hooks.run(workspace, :prepare_workspace,
               runner: fn _hook, _context ->
                 Process.sleep(50)
                 :ok
               end
             )

    assert receipt.hook_ref == "prepare"
    assert receipt.status == :timed_out
  end

  defp tmp_dir do
    path =
      Path.join(
        System.tmp_dir!(),
        "mezzanine-workspace-hooks-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    File.mkdir_p!(path)
    path
  end
end
