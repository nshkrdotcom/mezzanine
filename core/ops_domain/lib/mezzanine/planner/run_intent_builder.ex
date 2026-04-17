defmodule Mezzanine.Planner.RunIntentBuilder do
  @moduledoc """
  Builds run intents from a work object and compiled policy.
  """

  alias Mezzanine.Policy.TypedConfig
  alias MezzanineOpsModel.Intent.RunIntent
  alias MezzanineOpsModel.{PolicyBundle, WorkObject}

  @spec build(WorkObject.t(), PolicyBundle.t()) :: {:ok, [RunIntent.t()]} | {:error, term()}
  def build(%WorkObject{} = work, %PolicyBundle{} = bundle) do
    run_profile = TypedConfig.run_profile(bundle)
    placement_profile = TypedConfig.placement_profile(bundle)
    capability_grants = TypedConfig.capability_grants(bundle)

    RunIntent.new(%{
      intent_id: "run:" <> work.work_id,
      program_id: work.program_id,
      work_id: work.work_id,
      capability: run_profile.capability || "work.execute",
      runtime_class: run_profile.runtime_class,
      placement: %{
        profile_id: placement_profile.profile_id,
        strategy: placement_profile.strategy,
        target_selector: placement_profile.target_selector,
        runtime_preferences: placement_profile.runtime_preferences,
        requested_target: run_profile.target
      },
      grant_profile: %{
        capability_ids: Enum.map(capability_grants, & &1.capability_id),
        grants: capability_grants
      },
      input: %{
        title: work.title,
        work_type: work.work_type,
        payload: work.normalized_payload,
        prompt_template: bundle.prompt_template
      },
      metadata: %{
        work_metadata: work.metadata,
        policy_bundle_id: bundle.bundle_id
      }
    })
    |> case do
      {:ok, intent} -> {:ok, [intent]}
      {:error, reason} -> {:error, reason}
    end
  end
end
