defmodule Mezzanine.Policy.PlacementRules do
  @moduledoc """
  Typed placement-profile compiler.
  """

  alias Mezzanine.Policy.Helpers
  alias MezzanineOpsModel.PlacementProfile

  @spec from_config(map()) :: {:ok, PlacementProfile.t()}
  def from_config(config) do
    placement = Helpers.section(config, :placement)
    workspace = Helpers.section(config, :workspace)

    PlacementProfile.new(%{
      profile_id: Helpers.value(placement, :profile_id, "default"),
      strategy: cast_strategy(Helpers.value(placement, :strategy, "affinity")),
      target_selector: Helpers.value(placement, :target_selector, %{}),
      runtime_preferences: Helpers.value(placement, :runtime_preferences, %{}),
      workspace_policy: workspace,
      metadata: Helpers.value(placement, :metadata, %{})
    })
  end

  defp cast_strategy(value) when value in [:affinity, :balanced, :pinned], do: value
  defp cast_strategy("balanced"), do: :balanced
  defp cast_strategy("pinned"), do: :pinned
  defp cast_strategy(_value), do: :affinity
end
