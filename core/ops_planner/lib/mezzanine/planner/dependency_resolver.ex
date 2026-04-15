defmodule Mezzanine.Planner.DependencyResolver do
  @moduledoc """
  Pure dependency readiness checks.
  """

  alias MezzanineOpsModel.WorkObject

  @terminal_statuses [:completed, :resolved]

  @spec ready_to_plan?(WorkObject.t()) :: boolean()
  def ready_to_plan?(%WorkObject{dependency_keys: dependency_keys}) do
    Enum.empty?(dependency_keys)
  end

  @spec blocked_by_dependencies?(list(map())) :: boolean()
  def blocked_by_dependencies?(dependencies) when is_list(dependencies) do
    Enum.any?(dependencies, fn dependency ->
      Map.get(dependency, :status, Map.get(dependency, "status")) not in @terminal_statuses
    end)
  end
end
