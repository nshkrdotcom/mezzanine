defmodule Mezzanine.Boundary.GenerationSpec do
  @moduledoc """
  One generated boundary artifact family plus its manual extension points.
  """

  @enforce_keys [:family, :generated_artifacts, :manual_extension_points]
  defstruct family: nil,
            generated_artifacts: [],
            manual_extension_points: []

  @type t :: %__MODULE__{
          family: atom(),
          generated_artifacts: [atom()],
          manual_extension_points: [atom()]
        }
end

defmodule Mezzanine.Boundary.GenerationManifest do
  @moduledoc """
  Freezes the generator-assisted boundary scaffolding posture for the neutral
  substrate.

  Generated code is allowed for repetitive adapters and DTO mapping. Policy,
  pack logic, and semantic interpretation remain manual.
  """

  alias Mezzanine.Boundary.GenerationSpec

  @service_families [:work_queries, :work_control, :operator_actions, :reviews, :installations]
  @generated_artifacts [:backend_behaviour, :request_mapper, :response_mapper, :fixture_builder]
  @manual_extension_points [:policy_hooks, :pack_specific_projection_logic, :semantic_adapters]

  @specs %{
    work_queries: %GenerationSpec{
      family: :work_queries,
      generated_artifacts: @generated_artifacts ++ [:projection_adapter, :projection_filters],
      manual_extension_points: @manual_extension_points
    },
    work_control: %GenerationSpec{
      family: :work_control,
      generated_artifacts: @generated_artifacts ++ [:command_adapter, :command_errors],
      manual_extension_points: @manual_extension_points
    },
    operator_actions: %GenerationSpec{
      family: :operator_actions,
      generated_artifacts: @generated_artifacts ++ [:action_adapter, :action_result_mapper],
      manual_extension_points: @manual_extension_points
    },
    reviews: %GenerationSpec{
      family: :reviews,
      generated_artifacts: @generated_artifacts ++ [:decision_adapter, :decision_result_mapper],
      manual_extension_points: @manual_extension_points
    },
    installations: %GenerationSpec{
      family: :installations,
      generated_artifacts: @generated_artifacts ++ [:binding_adapter, :binding_validation_mapper],
      manual_extension_points: @manual_extension_points
    }
  }

  @spec service_families() :: [atom()]
  def service_families, do: @service_families

  @spec generated_artifacts() :: [atom()]
  def generated_artifacts, do: @generated_artifacts

  @spec manual_extension_points() :: [atom()]
  def manual_extension_points, do: @manual_extension_points

  @spec fetch!(atom()) :: GenerationSpec.t()
  def fetch!(family), do: Map.fetch!(@specs, family)
end
