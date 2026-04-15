defmodule Mezzanine.Policy.Compiler do
  @moduledoc """
  Compiles raw policy bundles into typed, pure compiled form.
  """

  alias Mezzanine.Policy.ApprovalPosture
  alias Mezzanine.Policy.GrantResolver
  alias Mezzanine.Policy.Helpers
  alias Mezzanine.Policy.PlacementRules
  alias Mezzanine.Policy.RetryProfile
  alias Mezzanine.Policy.ReviewRules
  alias MezzanineOpsModel.PolicyBundle

  @spec compile(PolicyBundle.t()) ::
          {:ok, PolicyBundle.t()}
          | {:error, term()}
  def compile(%PolicyBundle{} = bundle) do
    with {:ok, approval_posture} <- ApprovalPosture.from_config(bundle.config),
         {:ok, retry_profile} <- RetryProfile.from_config(bundle.config),
         {:ok, placement_profile} <- PlacementRules.from_config(bundle.config),
         {:ok, review_rules} <- ReviewRules.from_config(bundle.config),
         {:ok, capability_grants} <- GrantResolver.from_config(bundle.config) do
      {:ok,
       %PolicyBundle{
         bundle
         | compiled_form: %{
             run_profile: compile_run_profile(bundle.config),
             approval_posture: approval_posture,
             retry_profile: retry_profile,
             placement_profile: placement_profile,
             review_rules: review_rules,
             capability_grants: capability_grants
           }
       }}
    end
  end

  defp compile_run_profile(config) do
    run = Helpers.section(config, :run)

    %{
      profile_id: Helpers.value(run, :profile, "default"),
      runtime_class: cast_runtime_class(Helpers.value(run, :runtime_class, "session")),
      capability: Helpers.value(run, :capability),
      target: Helpers.value(run, :target)
    }
  end

  defp cast_runtime_class(value) when value in [:direct, :session, :stream], do: value
  defp cast_runtime_class("direct"), do: :direct
  defp cast_runtime_class("stream"), do: :stream
  defp cast_runtime_class(_value), do: :session
end
