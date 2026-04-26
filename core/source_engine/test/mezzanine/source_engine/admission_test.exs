defmodule Mezzanine.SourceEngine.AdmissionTest do
  use ExUnit.Case, async: true

  alias Mezzanine.SourceEngine.Admission
  alias Mezzanine.SourceEngine.SourceBinding
  alias Mezzanine.SourceEngine.SourceEvent

  test "admits a normalized source event with stable ids and hashes" do
    attrs = %{
      installation_id: "installation-1",
      source_binding_id: "linear-primary",
      provider: "linear",
      external_ref: "LIN-101",
      event_kind: "issue.updated",
      provider_revision: "2026-04-25T10:00:00Z",
      payload_schema: "linear.issue.v1",
      normalized_payload: %{"title" => "Ship source engine", "labels" => ["ops"]},
      trace_id: "trace-1",
      causation_id: "cause-1"
    }

    assert {:ok, %SourceEvent{} = event, seen} = Admission.admit(attrs, MapSet.new())

    assert event.contract_version == "Mezzanine.SourceEvent.v1"
    assert event.source_event_id =~ "src_"

    assert event.idempotency_key ==
             "linear/linear-primary/LIN-101/issue.updated/2026-04-25T10:00:00Z"

    assert event.payload_hash =~ "sha256:"
    assert event.status == :accepted
    assert MapSet.member?(seen, event.idempotency_key)
  end

  test "dedupes poll and webhook facts with the same provider revision" do
    attrs = %{
      installation_id: "installation-1",
      source_binding_id: "linear-primary",
      provider: "linear",
      external_ref: "LIN-101",
      event_kind: "issue.updated",
      provider_revision: "rev-1",
      payload_schema: "linear.issue.v1",
      normalized_payload: %{"state" => "Todo"},
      trace_id: "trace-1",
      causation_id: "cause-1"
    }

    assert {:ok, first, seen} = Admission.admit(attrs, MapSet.new())
    assert {:duplicate, duplicate, ^seen} = Admission.admit(attrs, seen)

    assert duplicate.source_event_id == first.source_event_id
    assert duplicate.status == :duplicate
  end

  test "rejects missing provider object identity" do
    attrs = %{
      installation_id: "installation-1",
      source_binding_id: "linear-primary",
      provider: "linear",
      event_kind: "issue.updated",
      provider_revision: "rev-1",
      payload_schema: "linear.issue.v1",
      normalized_payload: %{},
      trace_id: "trace-1",
      causation_id: "cause-1"
    }

    assert {:error, {:missing_required, :external_ref}} = Admission.admit(attrs, MapSet.new())
  end

  test "keeps Todo candidates blocked by non-terminal source blockers out of dispatch" do
    binding = source_binding()

    assert {:candidate, decision} =
             Admission.classify_candidate(
               %{
                 "state" => "Todo",
                 "assigned_to_worker" => true,
                 "blocked_by" => [
                   %{"external_ref" => "LIN-100", "state" => "In Progress"}
                 ]
               },
               binding
             )

    assert decision.lifecycle_state == "candidate"
    assert decision.reason == :blocked_by_non_terminal
    assert [%{"external_ref" => "LIN-100"}] = decision.blocker_refs
  end

  test "submits candidates whose blockers are terminal" do
    binding = source_binding()

    assert {:submitted, decision} =
             Admission.classify_candidate(
               %{
                 "state" => "Todo",
                 "assigned_to_worker" => true,
                 "blocked_by" => [
                   %{"external_ref" => "LIN-100", "state" => "Done"}
                 ]
               },
               binding
             )

    assert decision.lifecycle_state == "submitted"
    assert decision.reason == :dispatchable
    assert decision.blocker_refs == []
  end

  test "ignores source candidates that are not routed to this worker" do
    binding = source_binding()

    assert {:ignored, decision} =
             Admission.classify_candidate(
               %{"state" => "Todo", "assigned_to_worker" => false},
               binding
             )

    assert decision.reason == :not_routed_to_worker
  end

  defp source_binding do
    %SourceBinding{
      source_binding_id: "linear-primary",
      installation_id: "installation-1",
      provider: "linear",
      connection_ref: "linear-primary",
      state_mapping: %{
        "submitted" => ["Todo"],
        "completed" => ["Done"],
        "rejected" => ["Canceled", "Duplicate"]
      }
    }
  end
end
