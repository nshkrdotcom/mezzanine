defmodule Mezzanine.Lifecycle.SourceExecutionMapperTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Lifecycle.SourceExecutionMapper
  alias Mezzanine.WorkflowExecutionLifecycleInput

  test "canonicalizes admitted Linear source payloads" do
    payload = SourceExecutionMapper.canonical_linear_source_payload(source())

    assert payload.provider == "linear"
    assert payload.source_kind == "linear_issue"
    assert payload.source_binding_ref == "linear-primary"
    assert payload.source_ref == "linear://issue/LIN-101"
    assert payload.external_ref == "LIN-101"
    assert payload.source_state == "Todo"
  end

  test "maps admitted source to execution attempt input with required authority and lower refs" do
    assert {:ok, %WorkflowExecutionLifecycleInput{} = input} =
             SourceExecutionMapper.to_execution_attempt_input(source())

    assert input.tenant_ref == "tenant-1"
    assert input.installation_ref == "installation://inst-1@7"
    assert input.subject_ref["id"] == "subject-1"
    assert input.subject_ref["source_binding_ref"] == "linear-primary"
    assert input.trace_id == "trace-1"
    assert input.idempotency_key == "tenant-1:inst-1:linear-primary:LIN-101:rev-1"
    assert input.lower_idempotency_key == input.idempotency_key
    assert input.authority_packet_ref == "citadel-authority-request://subject-1"
    assert input.permission_decision_ref == "citadel-permission-decision://subject-1"
    assert input.routing_facts["installation_revision"] == 7
    assert input.routing_facts["capability"] == "linear.issue.execute"
    assert input.routing_facts["actor_ref"] == %{kind: "system", id: "source-admission"}
  end

  test "orders dispatchable sources by priority, opened time, and identifier" do
    result =
      SourceExecutionMapper.select_dispatch_candidates([
        source(%{
          external_ref: "LIN-003",
          identifier: "ENG-003",
          priority: 2,
          opened_at: "2026-03-12T09:30:00Z"
        }),
        source(%{
          external_ref: "LIN-001",
          identifier: "ENG-001",
          priority: 1,
          opened_at: "2026-03-12T10:00:00Z"
        }),
        source(%{
          external_ref: "LIN-002",
          identifier: "ENG-002",
          priority: 1,
          opened_at: "2026-03-12T09:00:00Z"
        })
      ])

    assert Enum.map(result.dispatchable, &Map.fetch!(&1, :identifier)) == [
             "ENG-002",
             "ENG-001",
             "ENG-003"
           ]

    assert result.held == []
  end

  test "rejects Todo with non-terminal dependencies before workflow input creation" do
    assert {:error, {:dispatch_preflight_rejected, fact}} =
             source(%{
               blocker_refs: [
                 %{
                   "provider_external_ref" => "LIN-099",
                   "source_ref" => "linear://issue/LIN-099",
                   "source_state" => "In Progress"
                 }
               ]
             })
             |> SourceExecutionMapper.to_execution_attempt_input()

    assert fact["fact_kind"] == "dispatch_preflight_rejected"
    assert fact["dispatch_eligible"] == false
    assert fact["reason"] == "non_terminal_dependency"

    assert [
             %{
               "provider_external_ref" => "LIN-099",
               "source_ref" => "linear://issue/LIN-099",
               "source_state" => "In Progress"
             }
           ] = fact["dependency_refs"]
  end

  test "allows Todo with terminal dependencies through dispatch preflight" do
    assert {:ok, %WorkflowExecutionLifecycleInput{}} =
             source(%{
               blocker_refs: [
                 %{
                   "provider_external_ref" => "LIN-099",
                   "source_ref" => "linear://issue/LIN-099",
                   "source_state" => "Done"
                 }
               ]
             })
             |> SourceExecutionMapper.to_execution_attempt_input()
  end

  test "holds Backlog and unknown states with preflight projection facts" do
    assert {:error, {:dispatch_preflight_rejected, backlog_fact}} =
             source(%{state: "Backlog"})
             |> SourceExecutionMapper.to_lifecycle_advance()

    assert backlog_fact["fact_kind"] == "dispatch_preflight_rejected"
    assert backlog_fact["source_state"] == "Backlog"
    assert backlog_fact["reason"] == "source_state_not_dispatchable"
    assert backlog_fact["dispatch_eligible"] == false

    assert {:error, {:dispatch_preflight_rejected, unknown_fact}} =
             source(%{state: "Investigating"})
             |> SourceExecutionMapper.to_lifecycle_advance()

    assert unknown_fact["source_state"] == "Investigating"
    assert unknown_fact["reason"] == "unknown_source_state"
    assert unknown_fact["dispatch_eligible"] == false
  end

  test "maps admitted source to existing lifecycle evaluator handoff instead of a new queue" do
    assert {:ok, handoff} = SourceExecutionMapper.to_lifecycle_advance(source())

    assert handoff.subject_id == "subject-1"
    assert handoff.facade == Mezzanine.LifecycleEvaluator
    assert handoff.handoff == :workflow_start_outbox
    assert handoff.opts[:installation_revision] == 7
    assert handoff.opts[:trigger] == :source_admission
    assert handoff.opts[:trace_id] == "trace-1"
  end

  test "rejects incomplete source records before workflow input creation" do
    assert {:error, {:missing_required_source_field, :tenant_id}} =
             source()
             |> Map.delete(:tenant_id)
             |> SourceExecutionMapper.to_execution_attempt_input()
  end

  defp source(overrides \\ %{}) do
    Map.merge(
      %{
        tenant_id: "tenant-1",
        installation_id: "inst-1",
        installation_revision: 7,
        subject_id: "subject-1",
        subject_kind: "linear_coding_ticket",
        source_binding_id: "linear-primary",
        provider: "linear",
        external_ref: "LIN-101",
        provider_revision: "rev-1",
        state: "Todo",
        trace_id: "trace-1",
        causation_id: "cause-1",
        actor_ref: %{kind: "system", id: "source-admission"},
        capability: "linear.issue.execute",
        normalized_payload: %{"title" => "Ship mapper"}
      },
      overrides
    )
  end
end
