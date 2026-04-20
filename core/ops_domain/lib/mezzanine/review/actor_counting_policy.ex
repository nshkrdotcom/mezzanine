defmodule Mezzanine.Review.ActorCountingPolicy do
  @moduledoc """
  Source-owned actor counting policy for review quorum resolution.

  Phase 5 keeps review hardening fail-closed: one actor counts once for quorum.
  Multi-role counting is only allowed when this source-owned module registers
  both the policy ref and the authority ref named by the normalized quorum
  profile. No such policy is registered in the current source tree.
  """

  @counting_rule "one_actor_counts_once"
  @source_owned_multi_role_policies %{}

  @type profile :: %{optional(atom()) => term()}
  @type evidence :: %{
          counting_rule: String.t(),
          multi_role_counting_allowed?: boolean(),
          multi_role_counting_policy_ref: String.t() | nil,
          multi_role_counting_authority_ref: String.t() | nil,
          source_owned_multi_role_policy_refs: [String.t()]
        }

  @spec counting_rule() :: String.t()
  def counting_rule, do: @counting_rule

  @spec source_owned_multi_role_policy_refs() :: [String.t()]
  def source_owned_multi_role_policy_refs, do: Map.keys(@source_owned_multi_role_policies)

  @spec multi_role_counting_allowed?(profile()) :: boolean()
  def multi_role_counting_allowed?(profile) when is_map(profile) do
    policy_ref = string_value(profile, :multi_role_counting_policy_ref)
    authority_ref = string_value(profile, :multi_role_counting_authority_ref)

    case Map.fetch(@source_owned_multi_role_policies, policy_ref) do
      {:ok, %{authority_ref: ^authority_ref}} when is_binary(authority_ref) -> true
      _other -> false
    end
  end

  @spec evidence(profile()) :: evidence()
  def evidence(profile) when is_map(profile) do
    %{
      counting_rule: @counting_rule,
      multi_role_counting_allowed?: multi_role_counting_allowed?(profile),
      multi_role_counting_policy_ref: string_value(profile, :multi_role_counting_policy_ref),
      multi_role_counting_authority_ref:
        string_value(profile, :multi_role_counting_authority_ref),
      source_owned_multi_role_policy_refs: source_owned_multi_role_policy_refs()
    }
  end

  defp string_value(map, key) when is_map(map) do
    case Map.get(map, key, Map.get(map, Atom.to_string(key))) do
      value when is_binary(value) and value != "" -> value
      value when is_atom(value) -> Atom.to_string(value)
      _other -> nil
    end
  end
end
