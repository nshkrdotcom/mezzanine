defmodule Mezzanine.GovernedEffectsTest do
  use Mezzanine.Execution.DataCase, async: false

  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Execution.LifecycleContinuation
  alias Mezzanine.GovernedEffects
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Reviews
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Work.{WorkClass, WorkObject}

  test "persists review-gated effect ambiguity and a reconciliation-only continuation" do
    fixture = effect_fixture()
    attrs = effect_attrs(fixture)

    assert {:ok, opened} = GovernedEffects.open(attrs)
    assert opened.status == :created
    assert opened.effect_record.status == "authorized"
    assert opened.execution.intent_snapshot["grant_ref"] == attrs.grant_ref
    assert opened.execution.dispatch_envelope["pinned_tool_manifest"]["manifest_ref"]

    assert {:error, {:review_not_accepted, :pending}} =
             GovernedEffects.begin_dispatch(opened.execution, %{
               expected_row_version: opened.execution.row_version,
               trace_id: attrs.trace_id,
               causation_id: "cause://dispatch-before-review"
             })

    assert {:ok, %{review_unit: accepted_review}} =
             Reviews.record_decision(fixture.tenant_id, fixture.review.id, %{
               decision: :accept,
               program_id: fixture.program.id,
               actor_kind: :human,
               actor_ref: "operator://p04",
               reason: "exact file operation reviewed",
               trace_id: attrs.trace_id,
               causation_id: "cause://review",
               idempotency_key: "p04-review-accept",
               expected_row_version: fixture.review.row_version
             })

    assert accepted_review.status == :accepted

    assert {:ok, dispatching} =
             GovernedEffects.begin_dispatch(opened.execution, %{
               expected_row_version: opened.execution.row_version,
               trace_id: attrs.trace_id,
               causation_id: "cause://dispatch"
             })

    assert dispatching.effect_record.status == "dispatching"

    assert {:ok, running} =
             GovernedEffects.record_accepted(dispatching.execution, %{
               expected_row_version: dispatching.execution.row_version,
               submission_ref: %{
                 "attempt_ref" => attrs.attempt_ref,
                 "external_ref" => "codex-thread://p04"
               },
               lower_receipt: %{"lower_receipt_ref" => "receipt://p04/accepted"},
               trace_id: attrs.trace_id,
               causation_id: "cause://accepted",
               actor_ref: attrs.actor_ref
             })

    assert running.effect_record.status == "running"

    receipt_attrs = %{
      expected_row_version: running.execution.row_version,
      receipt_state: "ambiguous",
      ambiguity_state: "outcome_unknown",
      receipt_ref: "receipt://p04/ambiguous",
      lower_receipt: %{"lower_event_ref" => "event://p04/provider-lost"},
      normalized_outcome: %{"reason" => "provider_connection_lost_after_dispatch"},
      trace_id: attrs.trace_id,
      causation_id: "cause://ambiguous",
      actor_ref: attrs.actor_ref,
      continuation_target: %{
        kind: "owner_command",
        owner: "jido_integration",
        command: "reconcile_effect_outcome",
        idempotency_key: "p04-reconcile-effect"
      }
    }

    assert {:ok, ambiguous} =
             GovernedEffects.record_receipt(running.execution, receipt_attrs)

    assert ambiguous.execution.dispatch_state == :stalled
    assert ambiguous.execution.next_dispatch_at == nil
    assert ambiguous.effect_record.status == "ambiguous"
    assert ambiguous.effect_record.ambiguity_state == "outcome_unknown"
    assert ambiguous.continuation.status == :pending
    assert ambiguous.continuation.target_transition == "reconcile_effect_outcome"

    assert {:ok, target} = LifecycleContinuation.dispatch_target(ambiguous.continuation)
    assert target["command"] == "reconcile_effect_outcome"

    assert ambiguous.continuation.metadata["retry_posture"] ==
             "reconciliation_only_effect_retry_prohibited"

    assert {:ok, replayed} =
             GovernedEffects.record_receipt(ambiguous.execution, receipt_attrs)

    assert replayed.status == :reused

    assert {:error, :terminal_effect_receipt_conflict} =
             GovernedEffects.record_receipt(
               ambiguous.execution,
               Map.put(receipt_attrs, :receipt_ref, "receipt://p04/conflict")
             )
  end

  test "rejects ambient credentials and blind retry commands before durable write" do
    fixture = effect_fixture()
    attrs = effect_attrs(fixture)

    assert {:error, {:forbidden_governed_effect_field, "api_key"}} =
             attrs
             |> Map.put(:api_key, "must-not-enter-mezzanine")
             |> GovernedEffects.open()

    assert {:ok, opened} = GovernedEffects.open(attrs)

    assert {:ok, %{review_unit: _accepted_review}} =
             Reviews.record_decision(fixture.tenant_id, fixture.review.id, %{
               decision: :accept,
               program_id: fixture.program.id,
               actor_kind: :human,
               actor_ref: "operator://p04",
               trace_id: attrs.trace_id,
               causation_id: "cause://review",
               idempotency_key: "p04-review-accept-negative"
             })

    assert {:ok, dispatching} =
             GovernedEffects.begin_dispatch(opened.execution, %{
               expected_row_version: opened.execution.row_version,
               trace_id: attrs.trace_id,
               causation_id: "cause://dispatch"
             })

    assert {:error, :attempt_identity_mismatch} =
             GovernedEffects.record_accepted(dispatching.execution, %{
               expected_row_version: dispatching.execution.row_version,
               submission_ref: %{"attempt_ref" => "attempt://wrong"},
               trace_id: attrs.trace_id,
               causation_id: "cause://wrong-attempt",
               actor_ref: attrs.actor_ref
             })

    assert {:error, {:forbidden_governed_effect_field, "api_key"}} =
             GovernedEffects.record_receipt(dispatching.execution, %{
               expected_row_version: dispatching.execution.row_version,
               receipt_state: "outcome_unknown",
               receipt_ref: "receipt://p04/secret",
               api_key: "must-not-enter-mezzanine",
               trace_id: attrs.trace_id,
               causation_id: "cause://secret",
               actor_ref: attrs.actor_ref,
               continuation_target: %{
                 kind: "owner_command",
                 owner: "jido_integration",
                 command: "reconcile_effect_outcome",
                 idempotency_key: "p04-secret"
               }
             })

    assert {:error, :ambiguous_effect_requires_reconciliation_command} =
             GovernedEffects.record_receipt(dispatching.execution, %{
               expected_row_version: dispatching.execution.row_version,
               receipt_state: "outcome_unknown",
               receipt_ref: "receipt://p04/blind-retry",
               trace_id: attrs.trace_id,
               causation_id: "cause://blind-retry",
               actor_ref: attrs.actor_ref,
               continuation_target: %{
                 kind: "owner_command",
                 owner: "jido_integration",
                 command: "retry_effect",
                 idempotency_key: "p04-blind-retry"
               }
             })
  end

  defp effect_fixture do
    tenant_id = "tenant-p04-#{System.unique_integer([:positive])}"
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "p04-#{System.unique_integer([:positive])}",
          name: "P04 Governed Effect",
          product_family: "operator_stack",
          configuration: %{},
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, policy_bundle} =
      PolicyBundle.load_bundle(
        %{
          program_id: program.id,
          name: "p04",
          version: "1.0.0",
          policy_kind: :workflow_md,
          source_ref: "WORKFLOW.md",
          body: "# P04 governed effect",
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_class} =
      WorkClass.create_work_class(
        %{
          program_id: program.id,
          name: "p04_effect_#{System.unique_integer([:positive])}",
          kind: "coding_task",
          intake_schema: %{"required" => ["title"]},
          policy_bundle_id: policy_bundle.id,
          default_review_profile: %{"required" => true},
          default_run_profile: %{"runtime" => "session"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_object} =
      WorkObject.ingest(
        %{
          program_id: program.id,
          work_class_id: work_class.id,
          external_ref: "app-kit:p04:#{System.unique_integer([:positive])}",
          title: "Reviewed Codex effect",
          description: "Create one reviewed file",
          priority: 50,
          source_kind: "app_kit",
          payload: %{"effect_ref" => "effect://p04"},
          normalized_payload: %{"effect_ref" => "effect://p04"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, control_session} =
      ControlSession.open(
        %{program_id: program.id, work_object_id: work_object.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, run_series} =
      RunSeries.open_series(
        %{work_object_id: work_object.id, control_session_id: control_session.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, run} =
      Run.schedule(
        %{
          run_series_id: run_series.id,
          attempt: 1,
          runtime_profile: %{"capability_id" => "codex.session.turn"},
          grant_profile: %{"effect_mode" => "managed_account_local_effect"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, review} =
      ReviewUnit.create_review_unit(
        %{
          work_object_id: work_object.id,
          run_id: run.id,
          review_kind: :code_review,
          decision_profile: %{"required_decisions" => 1},
          reviewer_actor: %{"kind" => "human", "ref" => "operator://p04"}
        },
        actor: actor,
        tenant: tenant_id
      )

    %{
      tenant_id: tenant_id,
      actor: actor,
      program: program,
      work_object: work_object,
      run: run,
      review: review
    }
  end

  defp effect_attrs(fixture) do
    suffix = System.unique_integer([:positive])

    %{
      tenant_id: fixture.tenant_id,
      installation_id: "installation://p04/local",
      subject_id: fixture.work_object.id,
      run_id: fixture.run.id,
      review_unit_id: fixture.review.id,
      effect_ref: "effect://p04/#{suffix}",
      run_ref: "run://p04/#{fixture.run.id}",
      turn_ref: "turn://p04/#{suffix}",
      command_ref: "command://p04/#{suffix}",
      decision_ref: "decision://citadel/p04/#{suffix}",
      grant_ref: "grant://citadel/p04/#{suffix}",
      review_ref: "review://mezzanine/#{fixture.review.id}",
      idempotency_key: "p04-effect-#{suffix}",
      target_ref: "target://codex/local/#{suffix}",
      trace_id: "trace://p04/#{suffix}",
      causation_id: "cause://p04/#{suffix}",
      attempt_ref: "attempt://p04/#{suffix}/1",
      pinned_tool_manifest: %{
        manifest_ref: "manifest://codex/p04/#{suffix}",
        manifest_hash: "sha256:" <> String.duplicate("a", 64),
        action_ids: ["create_or_replace_one_named_text_file"]
      },
      reviewed_operation: %{
        operation: "create_or_replace",
        workspace_ref: "workspace://p04/#{suffix}",
        file_ref: "file://p04/RESULT.txt",
        relative_path: "RESULT.txt",
        content_digest: "sha256:" <> String.duplicate("b", 64)
      },
      actor_ref: %{kind: "human", ref: "operator://p04"}
    }
  end
end
