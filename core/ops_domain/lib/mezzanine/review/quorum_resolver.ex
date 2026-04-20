defmodule Mezzanine.Review.QuorumResolver do
  @moduledoc """
  Review-owner quorum resolver over persisted review decision inputs.

  The resolver is pure: it evaluates `ReviewDecision` rows against the
  source-owned quorum profile and close-behavior matrix, then returns the
  terminal action, if any, for the caller that owns `ReviewUnit` mutation.
  """

  alias Mezzanine.Review.{QuorumProfile, ReviewDecision, ReviewUnit}

  @pending_result %{
    quorum_state: :pending,
    quorum_met?: false,
    terminal_action: nil,
    quorum_result: "pending"
  }

  @spec resolve(struct(), [struct()]) :: map()
  def resolve(%ReviewUnit{} = review_unit, decisions) when is_list(decisions) do
    profile = QuorumProfile.normalize(review_unit)
    behavior = QuorumProfile.close_behavior(profile)
    sorted_decisions = sort_decisions(decisions)
    accepted_decisions = decisions_for(sorted_decisions, :accept)
    rejected_decisions = decisions_for(sorted_decisions, :reject)

    base_result(profile, behavior, accepted_decisions, rejected_decisions)
    |> merge_resolution(
      profile,
      behavior,
      sorted_decisions,
      accepted_decisions,
      rejected_decisions
    )
  end

  defp merge_resolution(
         result,
         profile,
         behavior,
         sorted_decisions,
         accepted_decisions,
         rejected_decisions
       ) do
    resolution =
      cond do
        single_decision?(profile) ->
          single_decision_resolution(sorted_decisions)

        reject_met?(profile, rejected_decisions, accepted_decisions) ->
          terminal_result(:reject, "reject_policy_satisfied")

        accept_met?(profile, accepted_decisions) ->
          terminal_result(:accept, "accept_condition_satisfied")

        true ->
          @pending_result
      end

    Map.merge(result, resolution)
    |> Map.put(:release_manifest_ref, behavior.release_manifest_ref)
  end

  defp base_result(profile, behavior, accepted_decisions, rejected_decisions) do
    %{
      quorum_mode: profile.quorum_mode,
      close_owner: behavior.close_owner,
      close_behavior_ref: behavior.release_manifest_ref,
      required_decision_count: required_count(profile),
      accepted_decision_refs: decision_refs(accepted_decisions),
      rejected_decision_refs: decision_refs(rejected_decisions),
      accepted_actor_refs: distinct_actor_refs(accepted_decisions),
      rejected_actor_refs: distinct_actor_refs(rejected_decisions),
      counted_decision_refs: decision_refs(accepted_decisions ++ rejected_decisions),
      evaluated_at: DateTime.utc_now()
    }
  end

  defp single_decision_resolution(decisions) do
    decisions
    |> Enum.find(&(&1.decision in [:accept, :reject]))
    |> case do
      %ReviewDecision{decision: :accept} -> terminal_result(:accept, "first_eligible_accept")
      %ReviewDecision{decision: :reject} -> terminal_result(:reject, "first_eligible_reject")
      nil -> @pending_result
    end
  end

  defp terminal_result(:accept, reason) do
    %{
      quorum_state: :accepted,
      quorum_met?: true,
      terminal_action: :accept,
      quorum_result: "accepted",
      reason: reason
    }
  end

  defp terminal_result(:reject, reason) do
    %{
      quorum_state: :rejected,
      quorum_met?: false,
      terminal_action: :reject,
      quorum_result: "rejected",
      reason: reason
    }
  end

  defp single_decision?(%{quorum_mode: "single_decision"}), do: true
  defp single_decision?(_profile), do: false

  defp accept_met?(%{quorum_mode: "m_of_n"} = profile, accepted_decisions) do
    accepted_decisions
    |> distinct_actor_refs()
    |> length()
    |> Kernel.>=(required_count(profile))
  end

  defp accept_met?(%{quorum_mode: "two_person"} = profile, accepted_decisions) do
    accepted_decisions
    |> distinct_actor_refs()
    |> length()
    |> Kernel.>=(max(required_count(profile), 2))
  end

  defp accept_met?(%{quorum_mode: "role_diverse_m_of_n"} = profile, accepted_decisions) do
    accepted_actor_count = length(distinct_actor_refs(accepted_decisions))

    accepted_actor_count >= required_count(profile) and
      required_role_groups_met?(profile, accepted_decisions)
  end

  defp accept_met?(%{quorum_mode: "unanimous"} = profile, accepted_decisions) do
    eligible_actors = normalize_refs(profile.eligible_actor_refs)
    accepted_actors = MapSet.new(distinct_actor_refs(accepted_decisions))

    eligible_actors != [] and Enum.all?(eligible_actors, &MapSet.member?(accepted_actors, &1))
  end

  defp accept_met?(%{quorum_mode: "all_required_roles"} = profile, accepted_decisions) do
    required_groups = normalize_refs(profile.required_role_groups)

    required_groups != [] and
      length(distinct_actor_refs(accepted_decisions)) >= length(required_groups) and
      required_role_groups_met?(profile, accepted_decisions)
  end

  defp accept_met?(_profile, _accepted_decisions), do: false

  defp reject_met?(%{quorum_mode: "unanimous"} = profile, rejected_decisions, _accepted_decisions) do
    eligible_actors = normalize_refs(profile.eligible_actor_refs)

    if eligible_actors == [] do
      rejected_decisions != []
    else
      rejected_decisions
      |> distinct_actor_refs()
      |> Enum.any?(&(&1 in eligible_actors))
    end
  end

  defp reject_met?(profile, rejected_decisions, accepted_decisions) do
    rejected_actor_count = length(distinct_actor_refs(rejected_decisions))

    case profile.reject_policy do
      "majority_reject" ->
        rejected_actor_count > length(distinct_actor_refs(accepted_decisions))

      "threshold_reject" ->
        rejected_actor_count >= required_count(profile)

      _any_reject_veto ->
        rejected_actor_count > 0
    end
  end

  defp required_role_groups_met?(profile, accepted_decisions) do
    required_groups = normalize_refs(profile.required_role_groups)
    accepted_groups = accepted_decisions |> Enum.flat_map(&role_refs/1) |> MapSet.new()

    required_groups != [] and Enum.all?(required_groups, &MapSet.member?(accepted_groups, &1))
  end

  defp required_count(%{required_decision_count: count, minimum_distinct_actors: minimum})
       when is_integer(count) and is_integer(minimum) do
    max(count, minimum)
  end

  defp decisions_for(decisions, decision), do: Enum.filter(decisions, &(&1.decision == decision))

  defp decision_refs(decisions), do: decisions |> Enum.map(& &1.id) |> Enum.reject(&is_nil/1)

  defp distinct_actor_refs(decisions) do
    decisions
    |> Enum.map(& &1.actor_ref)
    |> Enum.reject(&blank?/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp role_refs(%ReviewDecision{payload: payload}) when is_map(payload) do
    payload
    |> value(:role_groups, [])
    |> List.wrap()
    |> Kernel.++(List.wrap(value(payload, :role_refs, [])))
    |> normalize_refs()
  end

  defp role_refs(_decision), do: []

  defp normalize_refs(refs) when is_list(refs) do
    refs
    |> Enum.map(&normalize_ref/1)
    |> Enum.reject(&blank?/1)
    |> Enum.uniq()
  end

  defp normalize_refs(ref), do: normalize_refs([ref])

  defp normalize_ref(%{ref: ref}), do: normalize_ref(ref)
  defp normalize_ref(%{"ref" => ref}), do: normalize_ref(ref)
  defp normalize_ref(%{id: id}), do: normalize_ref(id)
  defp normalize_ref(%{"id" => id}), do: normalize_ref(id)
  defp normalize_ref(ref) when is_binary(ref), do: ref
  defp normalize_ref(ref) when is_atom(ref), do: Atom.to_string(ref)
  defp normalize_ref(_ref), do: nil

  defp sort_decisions(decisions) do
    Enum.sort_by(decisions, fn decision ->
      {sort_time(decision), decision.id || ""}
    end)
  end

  defp sort_time(%{decided_at: %DateTime{} = decided_at}),
    do: DateTime.to_unix(decided_at, :microsecond)

  defp sort_time(_decision), do: 0

  defp value(map, key, default) when is_map(map) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp blank?(nil), do: true
  defp blank?(""), do: true
  defp blank?(_value), do: false
end
