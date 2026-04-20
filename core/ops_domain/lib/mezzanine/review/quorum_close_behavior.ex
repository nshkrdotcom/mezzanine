defmodule Mezzanine.Review.QuorumCloseBehavior do
  @moduledoc """
  Source-owned close-behavior matrix for review quorum modes.

  This module specifies how each supported quorum mode is allowed to close. It
  does not evaluate `ReviewDecision` rows or transition `ReviewUnit` records;
  that remains owner-resolver work for the next hardening slice.
  """

  @required_fields [
    :quorum_mode,
    :close_owner,
    :close_state_on_accept,
    :close_state_on_reject,
    :accept_condition,
    :reject_condition,
    :counting_rule,
    :actor_scope,
    :role_scope,
    :required_decision_count_rule,
    :minimum_distinct_actors_rule,
    :pending_after_first_accept?,
    :fail_closed_when_scope_missing?,
    :waiver_close_path,
    :escalation_close_path,
    :release_manifest_ref
  ]

  @modes [
    "single_decision",
    "m_of_n",
    "two_person",
    "role_diverse_m_of_n",
    "unanimous",
    "all_required_roles"
  ]

  @behaviors %{
    "single_decision" => %{
      quorum_mode: "single_decision",
      close_owner: "review_owner_quorum_resolver",
      close_state_on_accept: "accepted",
      close_state_on_reject: "rejected",
      accept_condition: "first_eligible_accept",
      reject_condition: "reject_policy",
      counting_rule: "one_actor_counts_once",
      actor_scope: "eligible_actor_refs_or_reviewer_actor",
      role_scope: "not_required",
      required_decision_count_rule: "exactly_one",
      minimum_distinct_actors_rule: "exactly_one",
      pending_after_first_accept?: false,
      fail_closed_when_scope_missing?: false,
      waiver_close_path: "waive_review_owner_action",
      escalation_close_path: "escalate_review_owner_action",
      release_manifest_ref: "review_quorum_close_behaviors[0]"
    },
    "m_of_n" => %{
      quorum_mode: "m_of_n",
      close_owner: "review_owner_quorum_resolver",
      close_state_on_accept: "accepted",
      close_state_on_reject: "rejected",
      accept_condition: "accepted_distinct_actors_gte_required_decision_count",
      reject_condition: "reject_policy",
      counting_rule: "one_actor_counts_once",
      actor_scope: "eligible_actor_refs_or_reviewer_actor",
      role_scope: "not_required",
      required_decision_count_rule: "decision_profile.required_decisions",
      minimum_distinct_actors_rule: "max(profile.minimum_distinct_actors, required_decisions)",
      pending_after_first_accept?: true,
      fail_closed_when_scope_missing?: false,
      waiver_close_path: "waive_review_owner_action",
      escalation_close_path: "escalate_review_owner_action",
      release_manifest_ref: "review_quorum_close_behaviors[1]"
    },
    "two_person" => %{
      quorum_mode: "two_person",
      close_owner: "review_owner_quorum_resolver",
      close_state_on_accept: "accepted",
      close_state_on_reject: "rejected",
      accept_condition: "at_least_two_distinct_eligible_actors_accept",
      reject_condition: "reject_policy",
      counting_rule: "one_actor_counts_once",
      actor_scope: "eligible_actor_refs_or_reviewer_actor",
      role_scope: "not_required",
      required_decision_count_rule: "max(profile.required_decisions, 2)",
      minimum_distinct_actors_rule: "max(profile.minimum_distinct_actors, 2)",
      pending_after_first_accept?: true,
      fail_closed_when_scope_missing?: false,
      waiver_close_path: "waive_review_owner_action",
      escalation_close_path: "escalate_review_owner_action",
      release_manifest_ref: "review_quorum_close_behaviors[2]"
    },
    "role_diverse_m_of_n" => %{
      quorum_mode: "role_diverse_m_of_n",
      close_owner: "review_owner_quorum_resolver",
      close_state_on_accept: "accepted",
      close_state_on_reject: "rejected",
      accept_condition:
        "accepted_distinct_actors_gte_required_count_and_required_role_groups_met",
      reject_condition: "reject_policy",
      counting_rule: "one_actor_counts_once",
      actor_scope: "eligible_actor_refs_or_reviewer_actor",
      role_scope: "required_role_groups_must_each_have_accepted_actor_evidence",
      required_decision_count_rule: "decision_profile.required_decisions",
      minimum_distinct_actors_rule: "max(profile.minimum_distinct_actors, required_decisions)",
      pending_after_first_accept?: true,
      fail_closed_when_scope_missing?: true,
      waiver_close_path: "waive_review_owner_action",
      escalation_close_path: "escalate_review_owner_action",
      release_manifest_ref: "review_quorum_close_behaviors[3]"
    },
    "unanimous" => %{
      quorum_mode: "unanimous",
      close_owner: "review_owner_quorum_resolver",
      close_state_on_accept: "accepted",
      close_state_on_reject: "rejected",
      accept_condition: "every_eligible_actor_accepts",
      reject_condition: "any_eligible_reject_vetoes",
      counting_rule: "one_actor_counts_once",
      actor_scope: "eligible_actor_refs_required",
      role_scope: "optional_role_evidence_only",
      required_decision_count_rule: "count(eligible_actor_refs)",
      minimum_distinct_actors_rule: "count(eligible_actor_refs)",
      pending_after_first_accept?: true,
      fail_closed_when_scope_missing?: true,
      waiver_close_path: "waive_review_owner_action",
      escalation_close_path: "escalate_review_owner_action",
      release_manifest_ref: "review_quorum_close_behaviors[4]"
    },
    "all_required_roles" => %{
      quorum_mode: "all_required_roles",
      close_owner: "review_owner_quorum_resolver",
      close_state_on_accept: "accepted",
      close_state_on_reject: "rejected",
      accept_condition: "every_required_role_group_has_eligible_acceptance",
      reject_condition: "reject_policy",
      counting_rule: "one_actor_counts_once",
      actor_scope: "eligible_actor_refs_or_reviewer_actor",
      role_scope: "required_role_groups_must_each_have_accepted_actor_evidence",
      required_decision_count_rule: "count(required_role_groups)",
      minimum_distinct_actors_rule: "count(required_role_groups)",
      pending_after_first_accept?: true,
      fail_closed_when_scope_missing?: true,
      waiver_close_path: "waive_review_owner_action",
      escalation_close_path: "escalate_review_owner_action",
      release_manifest_ref: "review_quorum_close_behaviors[5]"
    }
  }

  @type t :: %{required(atom()) => term()}

  @spec required_fields() :: [atom()]
  def required_fields, do: @required_fields

  @spec modes() :: [String.t()]
  def modes, do: @modes

  @spec all() :: [t()]
  def all, do: Enum.map(@modes, &Map.fetch!(@behaviors, &1))

  @spec fetch(String.t() | atom()) :: {:ok, t()} | {:error, {:unsupported_quorum_mode, term()}}
  def fetch(mode) when is_atom(mode) or is_binary(mode) do
    normalized_mode = to_string(mode)

    case Map.fetch(@behaviors, normalized_mode) do
      {:ok, behavior} -> {:ok, behavior}
      :error -> {:error, {:unsupported_quorum_mode, mode}}
    end
  end

  @spec fetch!(String.t() | atom()) :: t()
  def fetch!(mode) do
    case fetch(mode) do
      {:ok, behavior} ->
        behavior

      {:error, {:unsupported_quorum_mode, unsupported_mode}} ->
        raise ArgumentError, "unsupported review quorum mode: #{inspect(unsupported_mode)}"
    end
  end
end
