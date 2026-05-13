defmodule Mezzanine.WorkspaceEngine.PathSafetyTest do
  use ExUnit.Case, async: true

  alias Mezzanine.WorkspaceEngine.Allocator
  alias Mezzanine.WorkspaceEngine.PathSafety
  alias Mezzanine.WorkspaceEngine.WorkspaceRecord

  test "slugifies subject identity into deterministic workspace paths" do
    assert PathSafety.slug("LIN-101: fix / unsafe path") == "LIN-101__fix___unsafe_path"
  end

  test "workspace keys preserve only portable issue-identifier characters" do
    assert PathSafety.slug("AZaz09._-") == "AZaz09._-"
    assert PathSafety.slug("MT/Det") == "MT_Det"
    assert PathSafety.slug("LIN 101:fix\tpath\nnext") == "LIN_101_fix_path_next"
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

  test "duplicate sanitized subject refs reuse the same directory" do
    root = tmp_dir()

    first_attrs = %{
      installation_id: "installation-1",
      subject_id: "subject-1",
      subject_ref: "task/A",
      workspace_root: root
    }

    second_attrs = %{
      installation_id: "installation-1",
      subject_id: "subject-2",
      subject_ref: "task:A",
      workspace_root: root
    }

    assert {:ok, %WorkspaceRecord{} = first} = Allocator.reserve(first_attrs)
    assert {:ok, %WorkspaceRecord{} = second} = Allocator.reserve(second_attrs)

    assert first.slug == "task_A"
    assert second.slug == "task_A"
    assert second.concrete_path == first.concrete_path
    assert second.reuse? == true
    assert second.logical_ref == "workspace:installation-1:subject-2"
  end

  test "projects opaque workspace refs without concrete paths" do
    root = tmp_dir()

    assert {:ok, record} =
             Allocator.reserve(%{
               installation_id: "installation-1",
               subject_id: "subject-1",
               subject_ref: "linear:LIN-101",
               workspace_root: root,
               cleanup_policy: :on_terminal
             })

    public_ref = WorkspaceRecord.public_ref(record)

    assert public_ref.id == "workspace://#{record.workspace_id}"
    assert public_ref.path_redacted? == true
    assert public_ref.metadata.safety_hash == record.safety_hash
    assert public_ref.metadata.cleanup_policy == :on_terminal
    refute Map.has_key?(public_ref, :concrete_path)
    refute Map.has_key?(public_ref, :concrete_root)
    refute String.contains?(inspect(public_ref), root)
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

  test "canonicalizes symlinked workspace roots before reserving subject directories" do
    base = tmp_dir()
    actual_root = Path.join(base, "actual-workspaces")
    linked_root = Path.join(base, "linked-workspaces")
    File.mkdir_p!(actual_root)
    File.ln_s!(actual_root, linked_root)

    assert {:ok, %WorkspaceRecord{} = workspace} =
             Allocator.reserve(%{
               installation_id: "installation-1",
               subject_id: "subject-1",
               subject_ref: "task-root-link",
               workspace_root: linked_root
             })

    expected_root = Path.expand(actual_root)
    expected_path = Path.join(expected_root, "task-root-link")

    assert workspace.concrete_root == expected_root
    assert workspace.concrete_path == expected_path
    assert File.dir?(expected_path)
    refute String.starts_with?(workspace.concrete_path, Path.expand(linked_root))
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
