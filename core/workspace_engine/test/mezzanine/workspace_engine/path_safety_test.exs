defmodule Mezzanine.WorkspaceEngine.PathSafetyTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkspaceEngine.Allocator
  alias Mezzanine.WorkspaceEngine.PathSafety
  alias Mezzanine.WorkspaceEngine.WorkspaceRecord

  test "slugifies subject identity into deterministic workspace paths" do
    assert PathSafety.slug("LIN-101: fix / unsafe path") == "LIN-101__fix___unsafe_path"
  end

  test "reserves a reusable per-subject local workspace" do
    root = tmp_dir()

    attrs = %{
      installation_id: "installation-1",
      subject_id: "subject-1",
      subject_ref: "linear:LIN-101",
      workspace_root: root,
      placement_kind: :local,
      cleanup_policy: :on_terminal
    }

    assert {:ok, %WorkspaceRecord{} = first} = Allocator.reserve(attrs)
    assert File.dir?(first.concrete_path)
    assert first.logical_ref == "workspace:installation-1:subject-1"

    assert {:ok, %WorkspaceRecord{} = second} = Allocator.reserve(attrs)
    assert second.concrete_path == first.concrete_path
    assert second.reuse? == true
  end

  test "replaces a stale non-directory target under the workspace root" do
    root = tmp_dir()
    stale = Path.join(root, "linear_LIN-101")
    File.write!(stale, "stale")

    assert {:ok, record} =
             Allocator.reserve(%{
               installation_id: "installation-1",
               subject_id: "subject-1",
               subject_ref: "linear:LIN-101",
               workspace_root: root
             })

    assert File.dir?(record.concrete_path)
    assert record.safety_status == :validated
  end

  test "rejects root-as-workspace and outside-root paths" do
    root = tmp_dir()

    assert {:error, :workspace_is_root} = PathSafety.validate(root, root)
    assert {:error, :outside_workspace_root} = PathSafety.validate(root, Path.dirname(root))
  end

  test "rejects symlink escapes under the workspace root" do
    root = tmp_dir()
    outside = tmp_dir()
    link = Path.join(root, "linear_LIN-999")
    File.ln_s!(outside, link)

    assert {:error, :symlink_escape} = PathSafety.validate(root, link)
  end

  test "rejects symlink ancestor escapes before directory creation" do
    root = tmp_dir()
    outside = tmp_dir()
    link = Path.join(root, "linked-parent")
    File.ln_s!(outside, link)

    assert {:error, :symlink_escape} =
             PathSafety.validate(root, Path.join(link, "linear_LIN-999"))
  end

  defp tmp_dir do
    path =
      Path.join(
        System.tmp_dir!(),
        "mezzanine-workspace-engine-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    File.mkdir_p!(path)
    path
  end
end
