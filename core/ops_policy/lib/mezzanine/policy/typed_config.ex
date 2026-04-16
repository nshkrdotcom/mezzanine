defmodule Mezzanine.Policy.TypedConfig do
  @moduledoc """
  Typed getters over compiled policy bundles.
  """

  alias Mezzanine.Policy.Helpers
  alias MezzanineOpsModel.{CapabilityGrant, PlacementProfile}
  alias MezzanineOpsModel.PolicyBundle

  @atomized_keys %{
    "approval_posture" => :approval_posture,
    "capability" => :capability,
    "capability_grants" => :capability_grants,
    "capability_id" => :capability_id,
    "constraints" => :constraints,
    "escalation_required" => :escalation_required,
    "gates" => :gates,
    "initial_backoff_ms" => :initial_backoff_ms,
    "max_attempts" => :max_attempts,
    "max_backoff_ms" => :max_backoff_ms,
    "metadata" => :metadata,
    "mode" => :mode,
    "placement_profile" => :placement_profile,
    "profile_id" => :profile_id,
    "required" => :required,
    "required_decisions" => :required_decisions,
    "review_rules" => :review_rules,
    "reviewers" => :reviewers,
    "retry_profile" => :retry_profile,
    "run_profile" => :run_profile,
    "runtime_class" => :runtime_class,
    "runtime_preferences" => :runtime_preferences,
    "scope" => :scope,
    "strategy" => :strategy,
    "target" => :target,
    "target_selector" => :target_selector,
    "workspace_policy" => :workspace_policy
  }

  @runtime_class_values %{
    "direct" => :direct,
    "session" => :session,
    "stream" => :stream
  }

  @approval_mode_values %{
    "manual" => :manual,
    "auto" => :auto,
    "escalated" => :escalated
  }

  @retry_strategy_values %{
    "none" => :none,
    "linear" => :linear,
    "exponential" => :exponential
  }

  @placement_strategy_values %{
    "affinity" => :affinity,
    "balanced" => :balanced,
    "pinned" => :pinned
  }

  @grant_mode_values %{
    "allow" => :allow,
    "deny" => :deny,
    "escalate" => :escalate
  }

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

    key_value
    |> atomize_map()
    |> normalize_section(key)
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
    Map.get(@atomized_keys, key, key)
  end

  defp normalize_section(%_struct{} = value, _key), do: value

  defp normalize_section(run_profile, :run_profile) when is_map(run_profile) do
    Map.update(run_profile, :runtime_class, :session, &normalize_runtime_class/1)
  end

  defp normalize_section(approval_posture, :approval_posture) when is_map(approval_posture) do
    Map.update(approval_posture, :mode, :manual, &normalize_approval_mode/1)
  end

  defp normalize_section(retry_profile, :retry_profile) when is_map(retry_profile) do
    retry_profile
    |> Map.update(:strategy, :none, &normalize_retry_strategy/1)
    |> Map.update(:max_attempts, 1, &normalize_integer/1)
    |> Map.update(:initial_backoff_ms, 0, &normalize_integer/1)
    |> Map.update(:max_backoff_ms, 0, &normalize_integer/1)
  end

  defp normalize_section(placement_profile, :placement_profile) when is_map(placement_profile) do
    placement_profile
    |> Map.update(:strategy, :affinity, &normalize_placement_strategy/1)
    |> Map.update(:workspace_policy, %{}, &normalize_workspace_policy/1)
  end

  defp normalize_section(review_rules, :review_rules) when is_map(review_rules) do
    Map.update(review_rules, :required_decisions, 0, &normalize_integer/1)
  end

  defp normalize_section(capability_grants, :capability_grants) when is_list(capability_grants) do
    Enum.map(capability_grants, &normalize_capability_grant/1)
  end

  defp normalize_section(value, _key), do: value

  defp normalize_capability_grant(%{} = grant) do
    Map.update(grant, :mode, :allow, &normalize_grant_mode/1)
  end

  defp normalize_capability_grant(grant), do: grant

  defp normalize_workspace_policy(%{} = workspace_policy) do
    case workspace_reuse_entry(workspace_policy) do
      nil -> workspace_policy
      {key, value} -> Map.put(workspace_policy, key, normalize_boolean_like(value))
    end
  end

  defp normalize_workspace_policy(workspace_policy), do: workspace_policy

  defp workspace_reuse_entry(workspace_policy) do
    cond do
      Map.has_key?(workspace_policy, :reuse) -> {:reuse, Map.fetch!(workspace_policy, :reuse)}
      Map.has_key?(workspace_policy, "reuse") -> {"reuse", Map.fetch!(workspace_policy, "reuse")}
      true -> nil
    end
  end

  defp normalize_runtime_class(value), do: normalize_enum(value, @runtime_class_values, :session)
  defp normalize_approval_mode(value), do: normalize_enum(value, @approval_mode_values, :manual)
  defp normalize_retry_strategy(value), do: normalize_enum(value, @retry_strategy_values, :none)

  defp normalize_placement_strategy(value),
    do: normalize_enum(value, @placement_strategy_values, :affinity)

  defp normalize_grant_mode(value), do: normalize_enum(value, @grant_mode_values, :allow)

  defp normalize_enum(value, _mapping, _default) when is_atom(value), do: value

  defp normalize_enum(value, mapping, default) when is_binary(value),
    do: Map.get(mapping, value, default)

  defp normalize_enum(_value, _mapping, default), do: default

  defp normalize_integer(value) when is_integer(value), do: value

  defp normalize_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> value
    end
  end

  defp normalize_integer(value), do: value

  defp normalize_boolean_like(value) when is_boolean(value), do: value
  defp normalize_boolean_like("true"), do: true
  defp normalize_boolean_like("false"), do: false
  defp normalize_boolean_like(value), do: value
end
