defmodule Mezzanine.Pack.DefaultDependencyPolicyTest do
  use ExUnit.Case

  alias Mezzanine.Pack.{CompiledOperationRole, DefaultDependencyPolicy}

  test "infers parallel dependencies only between read-only roles" do
    roles = [
      role("source_read", :source_read, 1),
      role("evidence_read", :evidence_collection, 2),
      role("runtime_write", :runtime_operation, 3)
    ]

    dependencies = DefaultDependencyPolicy.infer(roles)

    assert dependency(dependencies, "source_read", "evidence_read").relation == :parallel_allowed
    assert dependency(dependencies, "source_read", "runtime_write").relation == :blocks_on_success

    assert dependency(dependencies, "evidence_read", "runtime_write").relation ==
             :blocks_on_success
  end

  test "preserves the upstream role completion and failure policy on inferred dependencies" do
    roles = [
      role("optional_evidence", :evidence_collection, 1,
        completion_policy: :optional,
        failure_policy: :degrade
      ),
      role("publication", :source_write, 2)
    ]

    [dependency] = DefaultDependencyPolicy.infer(roles)

    assert dependency.completion_policy == :optional
    assert dependency.failure_policy == :degrade

    assert dependency.metadata == %{
             "inferred" => true,
             "inference_policy" => "empty_graph_conservative_projection_order"
           }
  end

  defp dependency(dependencies, from_role, to_role) do
    Enum.find(dependencies, &(&1.from_role == from_role and &1.to_role == to_role))
  end

  defp role(role_ref, operation_class, projection_order_key, opts \\ []) do
    %CompiledOperationRole{
      role_ref: role_ref,
      binding_ref: "#{role_ref}_binding",
      binding_kind: :source,
      operation_role: "run",
      operation_ref: "#{role_ref}_operation",
      operation_class: operation_class,
      projection_order_key: projection_order_key,
      completion_policy: Keyword.get(opts, :completion_policy, :required),
      failure_policy: Keyword.get(opts, :failure_policy, :fail_workflow)
    }
  end
end
