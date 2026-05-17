defmodule Mezzanine.Substrate.StructWeightTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Substrate.StructWeight

  test "records every required generic struct and boundary envelope" do
    required_modules = [
      Mezzanine.Substrate.OperationContext,
      Mezzanine.Substrate.OperationRequest,
      Mezzanine.Substrate.PayloadEnvelope,
      Mezzanine.Substrate.ResultEnvelope,
      Mezzanine.Substrate.ResolvedOperationPlan,
      Mezzanine.Substrate.GovernedInvocationEnvelope,
      Mezzanine.Substrate.ExecutionInstruction,
      Mezzanine.Substrate.OperationReceipt,
      Mezzanine.Substrate.OperationLineageEvent,
      Mezzanine.Substrate.OperationGroupReceipt,
      Mezzanine.Substrate.WorkflowRun,
      Mezzanine.Substrate.OperationDependency,
      Mezzanine.Substrate.OperationGraph
    ]

    modules =
      StructWeight.catalog()
      |> Enum.map(& &1.module)

    assert Enum.all?(required_modules, &(&1 in modules))
  end

  test "does not accept overweight structs without an explicit decision" do
    assert Enum.all?(StructWeight.catalog(), fn record ->
             record.enforced_count <= 10 and record.total_count <= 16 and
               record.decision in [:accepted, :split_not_needed]
           end)
  end

  test "classifies fields by responsibility" do
    operation_context =
      StructWeight.catalog()
      |> Enum.find(&(&1.module == Mezzanine.Substrate.OperationContext))

    assert operation_context.categories.identity == [
             :operation_context_ref,
             :actor_ref,
             :tenant_ref,
             :installation_ref,
             :trace_ref,
             :request_ref,
             :idempotency_key
           ]

    assert :metadata in operation_context.categories.metadata
  end
end
