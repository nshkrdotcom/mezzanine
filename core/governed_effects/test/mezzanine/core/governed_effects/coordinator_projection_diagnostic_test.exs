defmodule Mezzanine.Core.GovernedEffects.CoordinatorProjectionDiagnosticTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Core.GovernedEffects.Coordinator
  alias Mezzanine.Core.GovernedEffects.DiagnosticWorkflow
  alias Mezzanine.Core.GovernedEffects.Projection

  test "Coordinator runs the full governed-effect happy path" do
    assert {:ok, run} = Coordinator.propose(command())
    assert run.effect.status == :proposed

    assert {:ok, run} = Coordinator.authorize(run, authority_decision(:allow))
    assert run.effect.status == :authorized

    assert {:ok, run} = Coordinator.dispatch(run)
    assert run.effect.status == :dispatched
    assert run.invocation_envelope["operation"] == "echo"

    assert {:ok, run} = Coordinator.receive_receipt(run, receipt(:success))
    assert run.effect.status == :receipt_received

    assert {:ok, run} = Coordinator.reduce(run)
    assert run.effect.status == :reduced

    assert {:ok, run} = Coordinator.project(run)
    assert run.effect.status == :projected
    assert run.projection["status"] == "projected"

    assert {:ok, run} = Coordinator.complete(run)
    assert run.effect.status == :completed
    assert run.projection["status"] == "completed"
  end

  test "Coordinator denial path is terminal and produces no lower invocation" do
    {:ok, run} = Coordinator.propose(command())

    assert {:ok, denied} = Coordinator.deny(run, authority_decision(:deny))

    assert denied.effect.status == :denied
    assert denied.invocation_envelope == nil
    assert denied.projection["authority_decision"] == "deny"
    assert {:error, {:terminal_effect, :denied}} = Coordinator.dispatch(denied)
  end

  test "Coordinator supports legacy bypass for non opted-in product paths" do
    assert {:legacy_bypass, bypass} =
             Coordinator.propose(command(), governed_effects_enabled: false)

    assert bypass.reason == :governed_effects_not_enabled
    assert bypass.command.effect_ref == "effect://tenant-a/diagnostic/001"
  end

  test "Coordinator delegates authority and dispatch through injected boundary adapters" do
    parent = self()

    authority_adapter = fn run ->
      send(parent, {:authority_requested, run.effect.effect_ref})
      {:ok, authority_decision(:allow)}
    end

    dispatch_adapter = fn envelope ->
      send(parent, {:dispatch_requested, envelope["effect_ref"]})
      {:ok, Map.put(envelope, "dispatch_ref", "dispatch://tenant-a/diagnostic/001")}
    end

    {:ok, run} = Coordinator.propose(command())
    assert {:ok, run} = Coordinator.authorize(run, authority_adapter: authority_adapter)
    assert_received {:authority_requested, "effect://tenant-a/diagnostic/001"}

    assert {:ok, run} = Coordinator.dispatch(run, dispatch_adapter: dispatch_adapter)
    assert_received {:dispatch_requested, "effect://tenant-a/diagnostic/001"}
    assert run.invocation_envelope["dispatch_ref"] == "dispatch://tenant-a/diagnostic/001"
  end

  test "Coordinator maps Citadel AuthorityDecision fields to internal vocabulary" do
    assert {:ok, packet, metadata} =
             Coordinator.map_authority_decision(%{
               authority_ref: "authority://tenant-a/diagnostic/review",
               decision: "review_required",
               tenant_ref: "tenant-a",
               actor_ref: "actor://user/operator-a",
               command_ref: "command://tenant-a/diagnostic/001",
               trace_ref: "trace-tenant-a-diagnostic-001",
               decision_hash: "sha256:decision",
               boundary_class: "diagnostic",
               posture: "review_before_dispatch"
             })

    assert packet.decision == :review
    assert metadata["decision_hash"] == "sha256:decision"
    assert metadata["boundary_class"] == "diagnostic"
    assert metadata["posture"] == "review_before_dispatch"
  end

  test "DiagnosticWorkflow registers a non-coding operation and runs through the lifecycle" do
    assert DiagnosticWorkflow.registered_effect_type?("diagnostic.echo")

    assert {:ok, operation} =
             DiagnosticWorkflow.operation(:echo, %{"message" => "hello"})

    assert operation.effect_type == "diagnostic.echo"
    assert operation.operation == "echo"
    refute operation.operation == "code"

    assert {:ok, run} = DiagnosticWorkflow.run_echo(command())
    assert run.effect.status == :completed
    assert run.invocation_envelope["operation"] == "echo"
    assert run.projection["receipt_status"] == "success"
  end

  test "Projection produces product-safe readback without lower internals" do
    {:ok, run} = Coordinator.propose(command())
    {:ok, run} = Coordinator.authorize(run, authority_decision(:allow))
    {:ok, run} = Coordinator.dispatch(run)
    {:ok, run} = Coordinator.receive_receipt(run, receipt(:success))
    {:ok, run} = Coordinator.reduce(run)
    {:ok, run} = Coordinator.project(run)

    projection = Projection.product_safe(run)

    assert projection["effect_ref"] == "effect://tenant-a/diagnostic/001"
    assert projection["status"] == "projected"
    assert projection["authority_decision"] == "allow"
    assert projection["receipt_status"] == "success"
    assert projection["evidence_refs"] == ["evidence://tenant-a/diagnostic/001"]
    assert is_list(projection["timeline"])

    refute Map.has_key?(projection, "raw_credentials")
    refute Map.has_key?(projection, "lower_facts")
    refute Enum.any?(Map.values(projection), &is_pid/1)
  end

  defp command do
    %{
      effect_ref: "effect://tenant-a/diagnostic/001",
      effect_type: "diagnostic.echo",
      command_ref: "command://tenant-a/diagnostic/001",
      tenant_ref: "tenant-a",
      actor_ref: "actor://user/operator-a",
      trace_ref: "trace-tenant-a-diagnostic-001",
      operation: "echo",
      payload: %{"message" => "hello"}
    }
  end

  defp authority_decision(decision) do
    %{
      authority_ref: "authority://tenant-a/diagnostic/review",
      decision: decision,
      tenant_ref: "tenant-a",
      actor_ref: "actor://user/operator-a",
      command_ref: "command://tenant-a/diagnostic/001",
      trace_ref: "trace-tenant-a-diagnostic-001",
      decision_hash: "sha256:decision",
      boundary_class: "diagnostic",
      posture: "allow_diagnostic"
    }
  end

  defp receipt(status) do
    %{
      receipt_ref: "receipt://tenant-a/diagnostic/001",
      effect_ref: "effect://tenant-a/diagnostic/001",
      status: status,
      evidence_refs: ["evidence://tenant-a/diagnostic/001"],
      trace_ref: "trace-tenant-a-diagnostic-001",
      lower_facts: %{"redacted_lower_fact" => "present"}
    }
  end
end
