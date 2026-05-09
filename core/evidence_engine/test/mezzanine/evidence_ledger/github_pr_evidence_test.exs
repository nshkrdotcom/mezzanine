defmodule Mezzanine.EvidenceLedger.GitHubPrEvidenceTest do
  use ExUnit.Case, async: true

  alias Mezzanine.EvidenceLedger.GitHubPrEvidence
  alias Mezzanine.EvidenceLedger.Store
  alias Mezzanine.EvidenceLedger.Store.Memory

  setup do
    Memory.reset!()
    :ok
  end

  test "materializes GitHub PR evidence from governed lower dispatches" do
    assert {:ok, evidence} =
             GitHubPrEvidence.materialize(
               [
                 dispatch("github.pr.create", pr_output(), "artifact://github/pr-create"),
                 dispatch(
                   "github.pr.reviews.list",
                   reviews_output(),
                   "artifact://github/reviews"
                 ),
                 dispatch(
                   "github.pr.review_comments.list",
                   comments_output(),
                   "artifact://github/comments"
                 ),
                 dispatch(
                   "github.commit.statuses.get_combined",
                   statuses_output(),
                   "artifact://github/status"
                 ),
                 dispatch(
                   "github.check_runs.list_for_ref",
                   checks_output(),
                   "artifact://github/checks"
                 )
               ],
               evidence_attrs()
             )

    assert evidence.evidence_kind == "github_pr"
    assert evidence.content_ref == "github-pr://nshkrdotcom/extravaganza/17"
    assert evidence.installation_id == "inst-1"
    assert evidence.subject_id == "subject-1"
    assert evidence.execution_id == "exec-1"
    assert evidence.trace_id == "trace-1"

    metadata = evidence.metadata
    assert metadata.provider == "github"
    assert metadata.repo == "nshkrdotcom/extravaganza"
    assert metadata.pull_number == 17
    assert metadata.head == %{ref: "phase-7", sha: "head-sha"}
    assert metadata.base == %{ref: "main", sha: "base-sha"}
    assert metadata.authority_refs == ["authority-decision://github"]
    assert metadata.connector_manifest_refs == ["manifest://jido/connectors/github@local"]

    assert metadata.lower_receipt_refs == [
             "lower-receipt://github.pr.create",
             "lower-receipt://github.pr.reviews.list",
             "lower-receipt://github.pr.review_comments.list",
             "lower-receipt://github.commit.statuses.get_combined",
             "lower-receipt://github.check_runs.list_for_ref"
           ]

    assert metadata.artifact_refs == [
             "artifact://github/pr-create",
             "artifact://github/reviews",
             "artifact://github/comments",
             "artifact://github/status",
             "artifact://github/checks"
           ]

    assert Enum.map(metadata.capability_negotiation_receipts, & &1.capability_id) == [
             "github.pr.create",
             "github.pr.reviews.list",
             "github.pr.review_comments.list",
             "github.commit.statuses.get_combined",
             "github.check_runs.list_for_ref"
           ]

    assert metadata.feedback.review_count == 2
    assert metadata.feedback.review_comment_count == 1
    assert metadata.feedback.review_states == %{"APPROVED" => 1, "CHANGES_REQUESTED" => 1}
    assert metadata.feedback.rework_required? == true
    assert metadata.status.combined_state == "success"
    assert metadata.status.check_run_count == 1
    assert metadata.cleanup_policy.governed_operations == ["github.git.ref.delete"]
  end

  test "collect stores materialized GitHub PR evidence in the configured store" do
    assert {:ok, stored} =
             GitHubPrEvidence.collect(dispatch("github.pr.fetch", pr_output()), evidence_attrs())

    assert {:ok, fetched} = Store.fetch_record(stored.id)
    assert fetched.evidence_kind == "github_pr"
    assert fetched.metadata.content_ref == "github-pr://nshkrdotcom/extravaganza/17"
  end

  test "fails closed when dispatches contain no PR evidence" do
    assert {:error, :missing_github_pr_evidence} =
             GitHubPrEvidence.materialize(dispatch("github.pr.reviews.list", reviews_output()))
  end

  defp evidence_attrs do
    %{
      installation_id: "inst-1",
      subject_id: "subject-1",
      execution_id: "exec-1",
      trace_id: "trace-1",
      causation_id: "cause-1"
    }
  end

  defp dispatch(capability_id, output, artifact_ref \\ "artifact://github/pr") do
    %{
      output: output,
      artifact_refs: [artifact_ref],
      governed_lower_envelope: %{
        capability_id: capability_id,
        lower_runtime_kind: :direct_connector,
        lower_request_ref: "lower-request://#{capability_id}",
        authority_ref: "authority-decision://github",
        authority_decision_hash: String.duplicate("b", 64),
        connector_manifest_ref: "manifest://jido/connectors/github@local",
        connector_manifest_hash: "sha256:github",
        capability_negotiation_ref: "cap-neg://#{capability_id}",
        trace_id: "trace-1"
      },
      governed_lower_receipt: %{
        lower_receipt_ref: "lower-receipt://#{capability_id}",
        status: :succeeded
      }
    }
  end

  defp pr_output do
    %{
      repo: "nshkrdotcom/extravaganza",
      pull_number: 17,
      title: "Phase 7 evidence",
      state: "open",
      draft: false,
      merged: false,
      mergeable: true,
      html_url: "https://github.com/nshkrdotcom/extravaganza/pull/17",
      head: %{ref: "phase-7", sha: "head-sha"},
      base: %{ref: "main", sha: "base-sha"}
    }
  end

  defp reviews_output do
    %{
      repo: "nshkrdotcom/extravaganza",
      pull_number: 17,
      reviews: [
        %{review_id: 1, state: "APPROVED"},
        %{review_id: 2, state: "CHANGES_REQUESTED"}
      ]
    }
  end

  defp comments_output do
    %{
      repo: "nshkrdotcom/extravaganza",
      pull_number: 17,
      comments: [%{comment_id: 11, path: "lib/extravaganza.ex"}]
    }
  end

  defp statuses_output do
    %{
      repo: "nshkrdotcom/extravaganza",
      ref: "head-sha",
      state: "success",
      statuses: [%{context: "ci", state: "success"}]
    }
  end

  defp checks_output do
    %{
      repo: "nshkrdotcom/extravaganza",
      ref: "head-sha",
      check_runs: [%{name: "mix ci", status: "completed", conclusion: "success"}]
    }
  end
end
