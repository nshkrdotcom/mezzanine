defmodule Mezzanine.WorkspaceTest do
  use ExUnit.Case, async: true

  test "declares the initial package path" do
    assert "core/mezzanine_core" in Mezzanine.Workspace.package_paths()
  end

  test "exposes the workspace project globs" do
    assert "." in Mezzanine.Workspace.active_project_globs()
    assert "core/*" in Mezzanine.Workspace.active_project_globs()
  end
end
