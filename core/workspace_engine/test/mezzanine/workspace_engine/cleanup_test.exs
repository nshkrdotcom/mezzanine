defmodule Mezzanine.WorkspaceEngine.CleanupTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkspaceEngine.{Allocator, Cleanup}

  test "removes terminal workspaces and returns a redacted cleanup receipt" do
    root = tmp_dir()

    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        subject_ref: "linear:LIN-101",
        workspace_root: root,
        cleanup_policy: :on_terminal
      })

    File.write!(Path.join(workspace.concrete_path, "artifact.txt"), "done")

    assert {:ok, receipt} = Cleanup.remove(workspace)

    assert receipt.status == :removed
    assert receipt.removed? == true
    assert receipt.workspace_ref == "workspace://#{workspace.workspace_id}"
    assert receipt.cleanup_policy == :on_terminal
    assert receipt.path_redacted? == true
    assert receipt.safety_hash == workspace.safety_hash
    refute File.exists?(workspace.concrete_path)
    refute String.contains?(inspect(receipt), root)
  end

  test "skips deletion for non-cleanup policies and still emits a receipt" do
    root = tmp_dir()

    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: root,
        cleanup_policy: :never
      })

    assert {:ok, receipt} = Cleanup.remove(workspace)

    assert receipt.status == :skipped
    assert receipt.reason == :cleanup_policy_never
    assert receipt.removed? == false
    assert File.dir?(workspace.concrete_path)
    refute String.contains?(inspect(receipt), root)
  end

  test "denies symlink escapes before cleanup side effects" do
    root = tmp_dir()
    outside = tmp_dir()

    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: root,
        cleanup_policy: :on_terminal
      })

    File.rm_rf!(workspace.concrete_path)
    File.ln_s!(outside, workspace.concrete_path)

    assert {:error, {:cleanup_denied, receipt}} = Cleanup.remove(workspace)

    assert receipt.status == :denied
    assert receipt.reason == :symlink_escape
    assert receipt.removed? == false
    assert File.exists?(outside)
    refute String.contains?(inspect(receipt), root)
    refute String.contains?(inspect(receipt), outside)
  end

  test "before_remove hook failures continue cleanup with redacted receipt evidence" do
    root = tmp_dir()

    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: root,
        cleanup_policy: :on_terminal,
        hook_specs: [
          %{
            "hook_ref" => "pre-remove",
            "stage" => "before_remove",
            "timeout_ms" => 100,
            "on_error" => "halt"
          }
        ]
      })

    assert {:ok, receipt} =
             Cleanup.remove(workspace,
               runner: fn _hook, _context ->
                 {:error, %{stdout: "archive unavailable secret-token", stderr: root}}
               end,
               redactions: ["secret-token", root]
             )

    assert receipt.status == :removed
    assert receipt.reason == nil
    assert receipt.removed? == true
    assert [hook_receipt] = receipt.hook_receipts
    assert hook_receipt.stage == :before_remove
    assert hook_receipt.status == :failed
    assert hook_receipt.action == :halt
    assert hook_receipt.reason.stdout == "archive unavailable [REDACTED]"
    assert hook_receipt.reason.stderr == "[REDACTED]"
    refute File.exists?(workspace.concrete_path)
    refute String.contains?(inspect(receipt), root)
  end

  test "before_remove hook timeouts continue cleanup with timeout receipt evidence" do
    root = tmp_dir()

    {:ok, workspace} =
      Allocator.reserve(%{
        installation_id: "installation-1",
        subject_id: "subject-1",
        workspace_root: root,
        cleanup_policy: :on_terminal,
        hook_specs: [
          %{"hook_ref" => "pre-remove", "stage" => "before_remove", "timeout_ms" => 1}
        ]
      })

    assert {:ok, receipt} =
             Cleanup.remove(workspace,
               runner: fn _hook, _context ->
                 Process.sleep(50)
                 :ok
               end
             )

    assert receipt.status == :removed
    assert receipt.removed? == true
    assert [hook_receipt] = receipt.hook_receipts
    assert hook_receipt.stage == :before_remove
    assert hook_receipt.status == :timed_out
    assert hook_receipt.action == :continue
    refute File.exists?(workspace.concrete_path)
    refute String.contains?(inspect(receipt), root)
  end

  defp tmp_dir do
    path =
      Path.join(
        System.tmp_dir!(),
        "mezzanine-workspace-cleanup-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    File.mkdir_p!(path)
    path
  end
end
