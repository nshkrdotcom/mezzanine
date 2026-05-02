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

  test "runs four workspace hook stages with fatal and non-fatal defaults" do
    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: tmp_dir(),
        hook_specs: [
          %{"hook_ref" => "created", "stage" => "after_create", "timeout_ms" => 100},
          %{"hook_ref" => "pre-run", "stage" => "before_run", "timeout_ms" => 100},
          %{"hook_ref" => "post-run", "stage" => "after_run", "timeout_ms" => 100},
          %{"hook_ref" => "pre-remove", "stage" => "before_remove", "timeout_ms" => 100}
        ]
      })

    assert {:ok, [after_create]} = Hooks.run(workspace, :after_create)
    assert after_create.fatal? == true
    assert after_create.action == :continue

    assert {:ok, [before_run]} = Hooks.run(workspace, :before_run)
    assert before_run.fatal? == true
    assert before_run.action == :continue

    assert {:ok, [after_run]} =
             Hooks.run(workspace, :after_run,
               runner: fn _hook, _context -> {:error, :telemetry_unavailable} end
             )

    assert after_run.status == :failed
    assert after_run.fatal? == false
    assert after_run.action == :continue
    assert after_run.reason == :telemetry_unavailable

    assert {:ok, [before_remove]} = Hooks.run(workspace, :before_remove)
    assert before_remove.fatal? == true
  end

  test "fatal four-stage hooks fail closed" do
    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: tmp_dir(),
        hook_specs: [
          %{"hook_ref" => "pre-run", "stage" => "before_run", "timeout_ms" => 100}
        ]
      })

    assert {:error, {:hook_failed, receipt}} =
             Hooks.run(workspace, :before_run,
               runner: fn _hook, _context -> {:error, :missing_dependency} end
             )

    assert receipt.stage == :before_run
    assert receipt.fatal? == true
    assert receipt.action == :halt
  end

  test "hook receipts redact and truncate output" do
    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: tmp_dir(),
        hook_specs: [
          %{"hook_ref" => "post-run", "stage" => "after_run", "timeout_ms" => 100}
        ]
      })

    assert {:ok, [receipt]} =
             Hooks.run(workspace, :after_run,
               max_output_bytes: 24,
               redactions: ["secret-token"],
               runner: fn _hook, _context ->
                 {:ok, %{output: "prefix secret-token " <> String.duplicate("x", 80)}}
               end
             )

    assert receipt.status == :succeeded
    assert receipt.truncated? == true
    refute String.contains?(receipt.result.output, "secret-token")
    assert String.contains?(receipt.result.output, "[REDACTED]")
    assert byte_size(receipt.result.output) <= 24
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
