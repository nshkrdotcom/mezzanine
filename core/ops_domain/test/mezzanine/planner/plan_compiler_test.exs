defmodule Mezzanine.Planner.PlanCompilerTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Planner
  alias Mezzanine.Policy.{BundleLoader, Compiler}
  alias MezzanineOpsModel.WorkObject

  @fixture_path Path.expand("../../fixtures/workflow.md", __DIR__)

  test "compiles a work object and compiled policy into a full work plan" do
    assert {:ok, bundle} = BundleLoader.load_file(@fixture_path)
    assert {:ok, compiled_bundle} = Compiler.compile(bundle)

    assert {:ok, work} =
             WorkObject.new(%{
               work_id: "work-1",
               program_id: "program-1",
               work_type: "coding_task",
               title: "Implement governed intake",
               payload: %{"issue_id" => "ENG-101"},
               status: :pending,
               metadata: %{"tracker" => "linear"}
             })

    assert {:ok, plan} = Planner.compile(work, compiled_bundle)

    assert plan.work_id == work.work_id
    assert length(plan.derived_run_intents) == 1
    assert length(plan.derived_review_intents) == 1
    assert length(plan.obligations) == 1

    [run_intent] = plan.derived_run_intents
    [review_intent] = plan.derived_review_intents
    [obligation] = plan.obligations

    assert run_intent.capability == "linear.issue.execute"
    assert run_intent.runtime_class == :session
    assert run_intent.placement.profile_id == "default-placement"
    assert "linear.issue.read" in run_intent.grant_profile.capability_ids
    assert review_intent.gate == "operator"
    assert obligation.subject.review_intent_id == review_intent.intent_id
    assert plan.metadata.retry_profile.strategy == :exponential
  end

  test "omits review intents and obligations when policy does not require review" do
    assert {:ok, bundle} =
             BundleLoader.load_map(%{
               config: %{
                 "run" => %{"capability" => "work.execute"},
                 "review" => %{"required" => false}
               },
               prompt_template: "Do the work."
             })

    assert {:ok, compiled_bundle} = Compiler.compile(bundle)

    assert {:ok, work} =
             WorkObject.new(%{
               work_id: "work-2",
               program_id: "program-2",
               work_type: "request",
               title: "Handle request",
               payload: %{},
               status: :pending
             })

    assert {:ok, plan} = Planner.compile(work, compiled_bundle)

    assert plan.derived_review_intents == []
    assert plan.obligations == []
  end
end
