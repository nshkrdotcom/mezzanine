defmodule Mezzanine.Policy.TypedConfig do
  @moduledoc """
  Typed getters over compiled policy bundles.
  """

  alias Mezzanine.Policy.Helpers
  alias MezzanineOpsModel.{CapabilityGrant, PlacementProfile}
  alias MezzanineOpsModel.PolicyBundle

  @atomizable_keys MapSet.new([
                     "approval_posture",
                     "capability",
                     "capability_grants",
                     "capability_id",
                     "constraints",
                     "escalation_required",
                     "gates",
                     "initial_backoff_ms",
                     "max_attempts",
                     "max_backoff_ms",
                     "metadata",
                     "mode",
                     "placement_profile",
                     "profile_id",
                     "required",
                     "required_decisions",
                     "review_rules",
                     "reviewers",
                     "retry_profile",
                     "run_profile",
                     "runtime_class",
                     "runtime_preferences",
                     "scope",
                     "strategy",
                     "target",
                     "target_selector",
                     "workspace_policy"
                   ])

  @spec run_profile(PolicyBundle.t()) :: map()
  def run_profile(%PolicyBundle{} = bundle), do: fetch(bundle, :run_profile)

  @spec approval_posture(PolicyBundle.t()) :: map()
  def approval_posture(%PolicyBundle{} = bundle) do
    posture = fetch(bundle, :approval_posture)
    Map.put(posture, :escalation_required, Helpers.boolean(posture[:escalation_required], false))
  end

  @spec retry_profile(PolicyBundle.t()) :: map()
  def retry_profile(%PolicyBundle{} = bundle), do: fetch(bundle, :retry_profile)

  @spec placement_profile(PolicyBundle.t()) :: MezzanineOpsModel.PlacementProfile.t()
  def placement_profile(%PolicyBundle{} = bundle) do
    case fetch(bundle, :placement_profile) do
      %PlacementProfile{} = placement_profile -> placement_profile
      placement_profile -> PlacementProfile.new!(placement_profile)
    end
  end

  @spec review_rules(PolicyBundle.t()) :: map()
  def review_rules(%PolicyBundle{} = bundle) do
    rules = fetch(bundle, :review_rules)
    Map.put(rules, :required, Helpers.boolean(rules[:required], false))
  end

  @spec capability_grants(PolicyBundle.t()) :: [MezzanineOpsModel.CapabilityGrant.t()]
  def capability_grants(%PolicyBundle{} = bundle) do
    bundle
    |> fetch(:capability_grants)
    |> Enum.map(fn
      %CapabilityGrant{} = capability_grant -> capability_grant
      capability_grant -> CapabilityGrant.new!(capability_grant)
    end)
  end

  @spec prompt_template(PolicyBundle.t()) :: String.t()
  def prompt_template(%PolicyBundle{} = bundle), do: bundle.prompt_template

  defp fetch(%PolicyBundle{compiled_form: compiled_form}, key) do
    key_value =
      if Map.has_key?(compiled_form, key) do
        Map.fetch!(compiled_form, key)
      else
        Map.fetch!(compiled_form, bundle_key(key))
      end

    atomize_map(key_value)
  end

  defp bundle_key(key) when is_atom(key), do: Atom.to_string(key)

  defp atomize_map(%_struct{} = value), do: value
  defp atomize_map(value) when is_list(value), do: Enum.map(value, &atomize_map/1)

  defp atomize_map(value) when is_map(value) do
    Enum.reduce(value, %{}, fn {key, item}, acc ->
      Map.put(acc, atomize_key(key), atomize_map(item))
    end)
  end

  defp atomize_map(value), do: value

  defp atomize_key(key) when is_atom(key), do: key

  defp atomize_key(key) when is_binary(key) do
    if MapSet.member?(@atomizable_keys, key) do
      String.to_existing_atom(key)
    else
      key
    end
  end
end
