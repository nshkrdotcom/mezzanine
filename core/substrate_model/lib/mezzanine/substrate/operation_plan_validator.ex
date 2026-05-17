defmodule Mezzanine.Substrate.OperationPlanValidator do
  @moduledoc "Pure completeness checks for resolved operation plans."

  alias Mezzanine.Substrate.ResolvedOperationPlan

  @same_request_dispatch_fields [
    :binding_ref,
    :manifest_ref,
    :operation_ref,
    :operation_class,
    :adapter_ref,
    :credential_scope_ref,
    :side_effect_class,
    :input_schema_ref
  ]

  @spec validate_complete(ResolvedOperationPlan.t()) :: :ok | {:error, term()}
  def validate_complete(%ResolvedOperationPlan{} = plan) do
    case Enum.find(@same_request_dispatch_fields, &(not present?(Map.get(plan, &1)))) do
      nil -> :ok
      field -> {:error, {:incomplete_resolved_operation_plan, field}}
    end
  end

  @spec same_request_dispatch_fields() :: [atom()]
  def same_request_dispatch_fields, do: @same_request_dispatch_fields

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
