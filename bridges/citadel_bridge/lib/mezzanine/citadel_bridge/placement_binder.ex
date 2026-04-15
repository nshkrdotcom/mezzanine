defmodule Mezzanine.CitadelBridge.PlacementBinder do
  @moduledoc """
  Maps Mezzanine placement hints into the public Citadel host-ingress target
  and constraint shapes.
  """

  alias MezzanineOpsModel.Intent.RunIntent

  @spec bind(RunIntent.t(), map()) :: %{target: map(), constraints: map()}
  def bind(%RunIntent{} = intent, attrs \\ %{}) when is_map(attrs) do
    placement = Map.new(intent.placement)

    target = %{
      target_kind: string_value(attrs, :target_kind, "runtime_target"),
      target_id:
        string_value(attrs, :target_id, placement_value(placement, :target_id, "default-target")),
      service_id:
        string_value(
          attrs,
          :service_id,
          placement_value(placement, :service_id, "default-service")
        ),
      boundary_class:
        string_value(
          attrs,
          :boundary_class,
          placement_value(placement, :boundary_class, to_string(intent.runtime_class))
        ),
      session_mode_preference:
        atomish_value(
          attrs,
          :session_mode_preference,
          placement_value(placement, :session_mode_preference, :attached)
        ),
      coordination_mode_preference:
        atomish_value(
          attrs,
          :coordination_mode_preference,
          placement_value(placement, :coordination_mode_preference, :single_target)
        ),
      routing_tags: routing_tags(intent, attrs, placement)
    }

    constraints = %{
      boundary_requirement:
        atomish_value(
          attrs,
          :boundary_requirement,
          placement_value(placement, :boundary_requirement, :fresh_or_reuse)
        ),
      allowed_boundary_classes: [target.boundary_class],
      allowed_service_ids: [target.service_id],
      forbidden_service_ids: list_value(attrs, :forbidden_service_ids, []),
      max_steps: Map.get(attrs, :max_steps, 1),
      review_required: Map.get(attrs, :review_required, false)
    }

    %{target: target, constraints: constraints}
  end

  defp routing_tags(intent, attrs, placement) do
    list_value(
      attrs,
      :routing_tags,
      placement_value(placement, :routing_tags, [to_string(intent.runtime_class)])
    )
  end

  defp placement_value(placement, key, default) do
    Map.get(placement, key) || Map.get(placement, Atom.to_string(key)) || default
  end

  defp string_value(attrs, key, default) do
    Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key)) || default
  end

  defp atomish_value(attrs, key, default) do
    Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key)) || default
  end

  defp list_value(attrs, key, default) do
    case Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key)) || default do
      value when is_list(value) -> value
      value -> [value]
    end
  end
end
