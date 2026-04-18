defmodule Mezzanine.ConfigRegistry.LifecycleHintContract do
  @moduledoc false

  alias Mezzanine.ConfigRegistry.Installation
  alias Mezzanine.Pack.{CompiledPack, ExecutionRecipeSpec, Serializer}

  @spec validate(Installation.t(), map() | nil) :: :ok | {:error, term()}
  def validate(%Installation{} = installation, binding_config \\ nil) do
    with {:ok, loaded_installation} <-
           Ash.load(installation, [:pack_registration], domain: Mezzanine.ConfigRegistry),
         {:ok, %CompiledPack{} = compiled_pack} <-
           Serializer.deserialize_compiled(
             loaded_installation.pack_registration.compiled_manifest
           ) do
      compiled_pack
      |> violations(binding_config || loaded_installation.binding_config || %{})
      |> case do
        [] -> :ok
        violations -> {:error, {:lifecycle_hint_contract_violation, violations}}
      end
    end
  end

  @spec violations(CompiledPack.t(), map()) :: [map()]
  def violations(%CompiledPack{} = compiled_pack, binding_config) when is_map(binding_config) do
    normalized_bindings = normalize_map(binding_config)

    compiled_pack.manifest.execution_recipe_specs
    |> Enum.flat_map(&recipe_violations(&1, normalized_bindings))
  end

  defp recipe_violations(%ExecutionRecipeSpec{} = recipe, binding_config) do
    required_hints = normalize_string_list(recipe.required_lifecycle_hints)

    if required_hints == [] do
      []
    else
      recipe_ref = to_string(recipe.recipe_ref)
      binding = get_in(binding_config, ["execution_bindings", recipe_ref]) || %{}
      capability = normalize_map(Map.get(binding, "connector_capability", %{}))
      produced_hints = normalize_string_list(Map.get(capability, "produces_lifecycle_hints", []))
      missing_hints = required_hints -- produced_hints

      if missing_hints == [] do
        []
      else
        [
          %{
            recipe_ref: recipe_ref,
            required_lifecycle_hints: required_hints,
            missing_hints: missing_hints,
            capability_id: Map.get(capability, "capability_id"),
            capability_version: Map.get(capability, "version")
          }
        ]
      end
    end
  end

  defp normalize_string_list(values) when is_list(values) do
    values
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_string_list(_other), do: []

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_other), do: %{}

  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value), do: value
end
