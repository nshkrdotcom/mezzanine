defmodule Mezzanine.Build.WorkspaceContract do
  @moduledoc false

  @package_paths [
    "core/mezzanine_core"
  ]

  @active_project_globs [".", "core/*", "apps/*", "bridges/*", "surfaces/*"]

  def package_paths, do: @package_paths
  def active_project_globs, do: @active_project_globs
end
