defmodule Mezzanine.Pack.DefaultDependencyPolicy do
  @moduledoc false

  alias Mezzanine.Pack.{CompiledOperationDependency, CompiledOperationRole}

  @inference_policy "empty_graph_conservative_projection_order"
  @read_only_operation_classes [:source_read, :evidence_collection]

  @spec infer([CompiledOperationRole.t()]) :: [CompiledOperationDependency.t()]
  def infer(roles) when is_list(roles) do
    roles
    |> Enum.with_index()
    |> Enum.flat_map(fn {from_role, index} ->
      roles
      |> Enum.drop(index + 1)
      |> Enum.map(&infer_dependency(from_role, &1))
    end)
  end

  defp infer_dependency(from_role, to_role) do
    %CompiledOperationDependency{
      from_role: from_role.role_ref,
      to_role: to_role.role_ref,
      relation: relation(from_role, to_role),
      completion_policy: from_role.completion_policy,
      failure_policy: from_role.failure_policy,
      metadata: %{
        "inferred" => true,
        "inference_policy" => @inference_policy
      }
    }
  end

  defp relation(from_role, to_role) do
    if read_only_operation_role?(from_role) and read_only_operation_role?(to_role) do
      :parallel_allowed
    else
      :blocks_on_success
    end
  end

  defp read_only_operation_role?(role) do
    role.operation_class in @read_only_operation_classes
  end
end
