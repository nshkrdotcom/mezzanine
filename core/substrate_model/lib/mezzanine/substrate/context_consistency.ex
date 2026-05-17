defmodule Mezzanine.Substrate.ContextConsistency do
  @moduledoc "Pure consistency checks for denormalized boundary fields."

  alias Mezzanine.Substrate.OperationContext

  @checked_fields [
    :tenant_ref,
    :installation_ref,
    :trace_ref,
    :actor_ref,
    :idempotency_key,
    :binding_set_ref,
    :run_binding_snapshot_ref
  ]

  @spec validate_boundary(OperationContext.t(), map()) :: :ok | {:error, term()}
  def validate_boundary(%OperationContext{} = context, boundary) when is_map(boundary) do
    case Enum.find(@checked_fields, &mismatch?(context, boundary, &1)) do
      nil -> :ok
      field -> {:error, {:context_mismatch, field}}
    end
  end

  defp mismatch?(context, boundary, field) do
    expected = Map.get(context, field)
    actual = Map.get(boundary, field)
    not is_nil(actual) and not is_nil(expected) and actual != expected
  end
end
