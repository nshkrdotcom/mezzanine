defmodule Mezzanine.Planner.PlanCompiler do
  @moduledoc """
  Pure compiler from `WorkObject + PolicyBundle` to `WorkPlan`.
  """

  alias Mezzanine.Planner.{ObligationDeriver, ReviewIntentBuilder, RunIntentBuilder}
  alias Mezzanine.Policy.TypedConfig
  alias MezzanineOpsModel.{PolicyBundle, WorkObject, WorkPlan}

  @spec compile(WorkObject.t(), PolicyBundle.t()) :: {:ok, WorkPlan.t()} | {:error, term()}
  def compile(%WorkObject{} = work, %PolicyBundle{} = bundle) do
    with {:ok, run_intents} <- RunIntentBuilder.build(work, bundle),
         {:ok, review_intents} <- ReviewIntentBuilder.build(work, bundle) do
      obligations =
        %WorkPlan{
          plan_id: plan_id(work, bundle),
          work_id: work.work_id,
          derived_run_intents: run_intents,
          derived_review_intents: review_intents
        }
        |> ObligationDeriver.derive()

      WorkPlan.new(%{
        plan_id: plan_id(work, bundle),
        work_id: work.work_id,
        derived_run_intents: run_intents,
        derived_review_intents: review_intents,
        derived_effect_intents: [],
        derived_read_intents: [],
        derived_notification_intents: [],
        obligations: obligations,
        metadata: %{
          policy_bundle_id: bundle.bundle_id,
          prompt_template: bundle.prompt_template,
          approval_posture: TypedConfig.approval_posture(bundle),
          retry_profile: TypedConfig.retry_profile(bundle),
          capability_grants: TypedConfig.capability_grants(bundle)
        }
      })
    end
  end

  defp plan_id(%WorkObject{work_id: work_id}, %PolicyBundle{bundle_id: bundle_id}) do
    "plan:" <> Integer.to_string(:erlang.phash2({work_id, bundle_id}))
  end
end
