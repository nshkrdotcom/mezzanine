defmodule Mezzanine.Workspace do
  @moduledoc """
  Introspection helpers for the Mezzanine workspace root.
  """

  @package_paths [
    "core/mezzanine_core"
  ]

  @active_project_globs [".", "core/*", "apps/*", "bridges/*", "surfaces/*"]

  @spec package_paths() :: [String.t()]
  def package_paths, do: @package_paths

  @spec active_project_globs() :: [String.t()]
  def active_project_globs, do: @active_project_globs
end
