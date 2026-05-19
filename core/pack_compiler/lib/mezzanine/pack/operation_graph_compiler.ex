defmodule Mezzanine.Pack.OperationGraphCompiler do
  @moduledoc false

  alias Mezzanine.Pack.{
    BindingSpec,
    CompiledOperationDependency,
    CompiledOperationGraph,
    CompiledOperationRole,
    DefaultDependencyPolicy,
    OperationDependency,
    OperationGraph,
    OperationRole
  }

  @spec compile_index([OperationGraph.t()], [BindingSpec.binding_record()]) ::
          %{String.t() => CompiledOperationGraph.t()}
  def compile_index(graphs, bindings) when is_list(graphs) and is_list(bindings) do
    bindings_by_ref = Map.new(bindings, &{&1.binding_ref, &1})

    Map.new(graphs, fn %OperationGraph{} = graph ->
      roles = Enum.map(graph.roles, &compile_operation_role(&1, bindings_by_ref))
      compiled_dependencies = compile_operation_dependencies(graph, roles)

      compiled = %CompiledOperationGraph{
        graph_ref: graph.graph_ref,
        workflow_ref: graph.workflow_ref,
        roles: roles,
        roles_by_ref: Map.new(roles, &{&1.role_ref, &1}),
        dependencies: compiled_dependencies,
        joins: graph.joins,
        metadata: graph.metadata
      }

      {compiled.graph_ref, compiled}
    end)
  end

  @spec compile_operation_role(OperationRole.t(), %{String.t() => BindingSpec.binding_record()}) ::
          CompiledOperationRole.t()
  defp compile_operation_role(%OperationRole{} = role, bindings_by_ref) do
    binding = Map.fetch!(bindings_by_ref, role.binding_ref)

    %CompiledOperationRole{
      role_ref: role.role_ref,
      binding_ref: role.binding_ref,
      binding_kind: BindingSpec.kind(binding),
      operation_role: role.operation_role,
      operation_ref: Map.fetch!(binding.operation_refs, role.operation_role),
      operation_class: role.operation_class,
      projection_order_key: role.projection_order_key,
      completion_policy: role.completion_policy,
      failure_policy: role.failure_policy,
      metadata: role.metadata
    }
  end

  @spec compile_operation_dependencies(OperationGraph.t(), [CompiledOperationRole.t()]) :: [
          CompiledOperationDependency.t()
        ]
  defp compile_operation_dependencies(%OperationGraph{dependencies: []}, roles) do
    roles
    |> Enum.sort_by(&{&1.projection_order_key, &1.role_ref})
    |> DefaultDependencyPolicy.infer()
  end

  defp compile_operation_dependencies(%OperationGraph{} = graph, _roles) do
    Enum.map(graph.dependencies, &compile_operation_dependency/1)
  end

  @spec compile_operation_dependency(OperationDependency.t()) :: CompiledOperationDependency.t()
  defp compile_operation_dependency(%OperationDependency{} = dependency) do
    %CompiledOperationDependency{
      from_role: dependency.from_role,
      to_role: dependency.to_role,
      relation: dependency.relation,
      completion_policy: dependency.completion_policy,
      failure_policy: dependency.failure_policy,
      review_policy_ref: dependency.review_policy_ref,
      confirmation_policy_ref: dependency.confirmation_policy_ref,
      metadata: dependency.metadata
    }
  end
end
