defmodule MezzanineOpsModel.ModelTest do
  use ExUnit.Case, async: true

  alias MezzanineOpsModel.AuditEventKind
  alias MezzanineOpsModel.Codec
  alias MezzanineOpsModel.EvidenceBundle
  alias MezzanineOpsModel.Intent.NotificationIntent
  alias MezzanineOpsModel.Intent.RunIntent
  alias MezzanineOpsModel.Normalizer
  alias MezzanineOpsModel.Obligation
  alias MezzanineOpsModel.PlacementProfile
  alias MezzanineOpsModel.PolicyBundle
  alias MezzanineOpsModel.ReviewStatus
  alias MezzanineOpsModel.ReviewUnit
  alias MezzanineOpsModel.Run
  alias MezzanineOpsModel.RunStatus
  alias MezzanineOpsModel.WorkObject
  alias MezzanineOpsModel.WorkPlan
  alias MezzanineOpsModel.WorkStatus

  test "constructs core structs with stable defaults" do
    run_intent =
      RunIntent.new!(%{
        intent_id: "intent-run-1",
        program_id: "program-1",
        work_id: "work-1",
        capability: "linear.issue.execute"
      })

    assert run_intent.runtime_class == :session
    assert run_intent.input == %{}

    work =
      WorkObject.new!(%{
        work_id: "work-1",
        program_id: "program-1",
        work_type: "issue_execution",
        title: "Execute issue",
        payload: %{issue: %{id: "ISS-1", labels: [:bug]}},
        status: :pending
      })

    assert work.status == :pending
    assert work.normalized_payload == %{"issue" => %{"id" => "ISS-1", "labels" => ["bug"]}}

    plan =
      WorkPlan.new!(%{
        plan_id: "plan-1",
        work_id: work.work_id,
        derived_run_intents: [run_intent]
      })

    assert [^run_intent] = plan.derived_run_intents

    run =
      Run.new!(%{
        run_id: "run-1",
        work_id: work.work_id,
        status: :scheduled,
        intent: run_intent
      })

    assert run.status == :scheduled

    review =
      ReviewUnit.new!(%{
        review_id: "review-1",
        work_id: work.work_id,
        status: :pending,
        gate: :operator
      })

    assert review.required_decisions == 1

    obligation =
      Obligation.new!(%{
        obligation_id: "obligation-1",
        work_id: work.work_id,
        obligation_type: :operator_review,
        state: :open
      })

    evidence = EvidenceBundle.new!(%{bundle_id: "bundle-1", work_id: work.work_id})

    placement =
      PlacementProfile.new!(%{
        profile_id: "placement-default",
        strategy: :affinity
      })

    policy =
      PolicyBundle.new!(%{
        bundle_id: "policy-1",
        source_ref: "WORKFLOW.md",
        config: %{"approval" => %{"mode" => "manual"}},
        prompt_template: "Do the work."
      })

    notification =
      NotificationIntent.new!(%{
        intent_id: "notify-1",
        channel: :operator_shell,
        audience: ["operator-1"]
      })

    assert obligation.state == :open
    assert evidence.summary == %{}
    assert placement.workspace_policy == %{}
    assert policy.compiled_form == %{}
    assert notification.payload == %{}
  end

  test "rejects invalid status values" do
    assert {:error, %ArgumentError{}} =
             WorkObject.new(%{
               work_id: "work-1",
               program_id: "program-1",
               work_type: "issue_execution",
               title: "Execute issue",
               payload: %{},
               status: :nonsense
             })

    assert {:error, %ArgumentError{}} =
             Run.new(%{
               run_id: "run-1",
               work_id: "work-1",
               status: "wat",
               intent:
                 RunIntent.new!(%{
                   intent_id: "intent-run-1",
                   program_id: "program-1",
                   work_id: "work-1",
                   capability: "linear.issue.execute"
                 })
             })
  end

  test "exposes exhaustive status vocabularies" do
    assert :planning in WorkStatus.values()
    assert :scheduled in RunStatus.values()
    assert :stalled in RunStatus.values()
    assert :accepted in ReviewStatus.values()
    assert :escalated in ReviewStatus.values()
    assert :work_planned in AuditEventKind.values()
    assert :review_created in AuditEventKind.values()
    assert :operator_paused in AuditEventKind.values()

    assert {:ok, :pending} = WorkStatus.cast("pending")
    assert {:ok, :cancelled} = WorkStatus.cast("canceled")
    assert {:ok, :running} = RunStatus.cast(:running)
    assert {:ok, :cancelled} = RunStatus.cast("canceled")
    assert {:ok, :accepted} = ReviewStatus.cast("approved")
    assert {:ok, :work_ingested} = AuditEventKind.cast("work_created")
  end

  test "deep-normalizes payloads and stays idempotent" do
    payload = %{issue: %{id: "ISS-1", labels: [:bug, :p1]}, tags: ["linear", :ops]}

    normalized = Normalizer.normalize_payload(payload)

    assert normalized == %{
             "issue" => %{"id" => "ISS-1", "labels" => ["bug", "p1"]},
             "tags" => ["linear", "ops"]
           }

    assert Normalizer.normalize_payload(normalized) == normalized
  end

  test "dumps pure structs into storage-safe maps" do
    run_intent =
      RunIntent.new!(%{
        intent_id: "intent-run-1",
        program_id: "program-1",
        work_id: "work-1",
        capability: "linear.issue.execute",
        runtime_class: :session,
        metadata: %{source: :planner}
      })

    dumped = Codec.dump(run_intent)

    assert dumped == %{
             "capability" => "linear.issue.execute",
             "grant_profile" => %{},
             "input" => %{},
             "intent_id" => "intent-run-1",
             "metadata" => %{"source" => "planner"},
             "placement" => %{},
             "program_id" => "program-1",
             "runtime_class" => "session",
             "work_id" => "work-1"
           }
  end
end
