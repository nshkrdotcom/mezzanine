defmodule Mezzanine.SourceEngine.AdmissionTest do
  use ExUnit.Case, async: true

  alias Mezzanine.SourceEngine.Admission
  alias Mezzanine.SourceEngine.LinearIssue
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

  test "classifies normalized Jido Linear issue maps with nested workflow state" do
    binding = source_binding()

    assert {:submitted, decision} =
             Admission.classify_candidate(
               %{
                 state: %{name: "Todo", type: "unstarted"},
                 assigned_to_worker: true,
                 blockers: [
                   %{
                     "provider_external_ref" => "lin-100",
                     "source_state" => "Done"
                   }
                 ]
               },
               binding
             )

    assert decision.lifecycle_state == "submitted"
    assert decision.reason == :dispatchable
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

  test "normalizes Linear issue source facts into installation-scoped subject attrs" do
    assert {:ok, attrs} =
             LinearIssue.subject_attrs(
               linear_issue(),
               source_envelope(),
               source_binding()
             )

    assert attrs.installation_id == "installation-1"
    assert attrs.source_ref == "linear://installation-1/issue/ENG-321"
    assert attrs.source_binding_id == "linear-primary"
    assert attrs.provider == "linear"
    assert attrs.provider_external_ref == "lin-issue-321"
    assert attrs.provider_revision == "2026-03-12T10:00:00Z"
    assert attrs.source_state == "Todo"
    assert attrs.lifecycle_state == "candidate"
    assert attrs.title == "Investigate deployment rollback"
    assert attrs.description == "The deployment rolled back after the health checks failed."
    assert attrs.priority == 2
    assert attrs.labels == ["automation", "incident"]
    assert attrs.branch_ref == "eng-321-investigate-rollback"
    assert attrs.source_url == "https://linear.app/acme/issue/ENG-321"
    assert %DateTime{} = attrs.opened_at

    assert attrs.payload == %{
             "identifier" => "ENG-321",
             "source_kind" => "linear",
             "title" => "Investigate deployment rollback"
           }

    assert attrs.state_mapping == %{
             admission_classification: "candidate",
             canonical_state: "submitted",
             lifecycle_state: "candidate",
             provider_state: "Todo",
             provider_state_type: "unstarted",
             reason: "blocked_by_non_terminal"
           }

    assert [
             %{
               "identifier" => "SEC-9",
               "provider_external_ref" => "lin-issue-009",
               "source_ref" => "linear://issue/SEC-9",
               "source_state" => "In Progress"
             }
           ] = attrs.blocker_refs

    assert attrs.source_routing["assignee"]["id"] == "usr-linear-viewer"
    assert attrs.source_routing["project"]["slug_id"] == "ops-automation"
    assert attrs.source_routing["team"]["key"] == "ENG"
    assert attrs.source_routing["provenance"]["source_ref"] == attrs.source_ref

    refute Map.has_key?(attrs, :tenant_id)
    refute Map.has_key?(attrs, :tenant_scope)
    refute Map.has_key?(attrs, :authorization_scope)
    refute Map.has_key?(attrs, :authority_ref)
  end

  test "rejects Linear subject normalization without tenant envelope evidence" do
    issue = linear_issue()
    binding = source_binding()

    assert {:error, :missing_tenant_scope} =
             LinearIssue.subject_attrs(issue, Map.delete(source_envelope(), :tenant_id), binding)

    assert {:error, :missing_installation_scope} =
             LinearIssue.subject_attrs(
               issue,
               Map.delete(source_envelope(), :installation_id),
               binding
             )

    assert {:error, :authorization_scope_missing} =
             LinearIssue.subject_attrs(
               issue,
               Map.delete(source_envelope(), :authorization_scope),
               binding
             )

    assert {:error, :authorization_scope_mismatch} =
             LinearIssue.subject_attrs(
               issue,
               %{source_envelope() | authorization_scope: %{"tenant_id" => "tenant-2"}},
               binding
             )
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

  defp source_envelope do
    %{
      tenant_id: "tenant-1",
      installation_id: "installation-1",
      source_binding_id: "linear-primary",
      authorization_scope: %{"tenant_id" => "tenant-1", "actor_id" => "source-ingest"},
      trace_id: "trace-linear-ENG-321",
      causation_id: "linear-event-ENG-321",
      actor_ref: %{"kind" => "system", "id" => "source-ingest", "tenant_id" => "tenant-1"}
    }
  end

  defp linear_issue do
    %{
      id: "lin-issue-321",
      identifier: "ENG-321",
      title: "Investigate deployment rollback",
      description: "The deployment rolled back after the health checks failed.",
      priority: 2,
      branch_name: "eng-321-investigate-rollback",
      labels: ["Incident", "automation"],
      url: "https://linear.app/acme/issue/ENG-321",
      created_at: "2026-03-12T09:15:00Z",
      updated_at: "2026-03-12T10:00:00Z",
      state: %{id: "state-todo", name: "Todo", type: "unstarted"},
      assignee: %{
        id: "usr-linear-viewer",
        name: "Taylor Automation",
        email: "taylor@example.test"
      },
      project: %{
        id: "project-ops",
        name: "Ops Automation",
        slug_id: "ops-automation",
        url: "https://linear.app/acme/project/ops-automation"
      },
      team: %{id: "team-eng", key: "ENG", name: "Engineering"},
      blockers: [
        %{
          id: "rel-blocks-001",
          type: "blocks",
          direction: "inbound",
          issue: %{
            id: "lin-issue-009",
            identifier: "SEC-9",
            title: "Restore deployment credentials",
            url: "https://linear.app/acme/issue/SEC-9",
            state: %{id: "state-progress", name: "In Progress", type: "started"}
          }
        }
      ]
    }
  end
end
