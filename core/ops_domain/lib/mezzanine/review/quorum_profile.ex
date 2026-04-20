defmodule Mezzanine.Review.QuorumProfile do
  @moduledoc """
  Normalizes review-quorum metadata carried by `ReviewUnit.decision_profile`.

  This module defines the explicit field set required before review terminal
  behavior can claim m-of-n, two-person, role-diverse, unanimous, or
  all-required-role semantics. It does not evaluate or close quorum by itself.
  """

  alias Mezzanine.Review.{QuorumCloseBehavior, ReviewUnit}

  @required_fields [
    :review_quorum_ref,
    :review_unit_id,
    :work_object_id,
    :review_kind,
    :decision_profile_hash,
    :quorum_mode,
    :required_decision_count,
    :eligible_actor_refs,
    :eligible_role_refs,
    :required_role_groups,
    :minimum_distinct_actors,
    :requester_actor_ref,
    :self_approval_allowed?,
    :duplicate_actor_policy,
    :reject_policy,
    :waiver_policy_ref,
    :escalation_policy_ref,
    :decision_idempotency_key,
    :actor_evidence_refs,
    :accepted_decision_refs,
    :rejected_decision_refs,
    :waived_decision_refs,
    :quorum_state,
    :quorum_met?,
    :quorum_result,
    :quorum_evaluated_at,
    :release_manifest_ref
  ]

  @quorum_modes [
    "single_decision",
    "m_of_n",
    "two_person",
    "role_diverse_m_of_n",
    "unanimous",
    "all_required_roles"
  ]

  @duplicate_actor_policies ["latest_only", "first_only", "same_actor_update"]
  @reject_policies ["any_reject_veto", "majority_reject", "threshold_reject"]

  @type t :: %{required(atom()) => term()}

  @spec required_fields() :: [atom()]
  def required_fields, do: @required_fields

  @spec quorum_modes() :: [String.t()]
  def quorum_modes, do: @quorum_modes

  @spec close_behavior(t() | struct() | String.t() | atom()) :: QuorumCloseBehavior.t()
  def close_behavior(%ReviewUnit{} = review_unit),
    do: review_unit |> normalize() |> close_behavior()

  def close_behavior(%{quorum_mode: quorum_mode}), do: QuorumCloseBehavior.fetch!(quorum_mode)

  def close_behavior(quorum_mode) when is_binary(quorum_mode) or is_atom(quorum_mode),
    do: QuorumCloseBehavior.fetch!(quorum_mode)

  @spec normalize(struct()) :: t()
  def normalize(%ReviewUnit{} = review_unit) do
    profile = normalize_profile(review_unit.decision_profile)
    profile_hash = decision_profile_hash(profile)
    quorum_mode = quorum_mode(profile)
    required_decision_count = required_decision_count(profile, quorum_mode)

    minimum_distinct_actors =
      minimum_distinct_actors(profile, quorum_mode, required_decision_count)

    %{
      review_quorum_ref: review_quorum_ref(review_unit, profile_hash),
      review_unit_id: review_unit.id,
      work_object_id: review_unit.work_object_id,
      review_kind: normalize_value(review_unit.review_kind),
      decision_profile_hash: profile_hash,
      quorum_mode: quorum_mode,
      required_decision_count: required_decision_count,
      eligible_actor_refs: eligible_actor_refs(profile, review_unit.reviewer_actor),
      eligible_role_refs: string_list(value(profile, "eligible_role_refs")),
      required_role_groups: list_value(value(profile, "required_role_groups")),
      minimum_distinct_actors: minimum_distinct_actors,
      requester_actor_ref: optional_string(value(profile, "requester_actor_ref")),
      self_approval_allowed?: boolean_value(value(profile, "self_approval_allowed"), false),
      duplicate_actor_policy:
        enum_value(
          value(profile, "duplicate_actor_policy"),
          @duplicate_actor_policies,
          "latest_only"
        ),
      reject_policy:
        enum_value(value(profile, "reject_policy"), @reject_policies, "any_reject_veto"),
      waiver_policy_ref: optional_string(value(profile, "waiver_policy_ref")),
      escalation_policy_ref: optional_string(value(profile, "escalation_policy_ref")),
      decision_idempotency_key: decision_idempotency_key(review_unit, quorum_mode, profile_hash),
      actor_evidence_refs: string_list(value(profile, "actor_evidence_refs")),
      accepted_decision_refs: string_list(value(profile, "accepted_decision_refs")),
      rejected_decision_refs: string_list(value(profile, "rejected_decision_refs")),
      waived_decision_refs: string_list(value(profile, "waived_decision_refs")),
      quorum_state: quorum_state(review_unit.status),
      quorum_met?: review_unit.status == :accepted,
      quorum_result: quorum_result(review_unit.status),
      quorum_evaluated_at: datetime_value(review_unit.updated_at || review_unit.inserted_at),
      release_manifest_ref: optional_string(value(profile, "release_manifest_ref"))
    }
  end

  defp normalize_profile(profile) when is_map(profile) do
    Map.new(profile, fn {key, nested_value} -> {to_string(key), normalize_value(nested_value)} end)
  end

  defp normalize_profile(_profile), do: %{}

  defp decision_profile_hash(profile) do
    profile
    |> Jason.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp review_quorum_ref(%ReviewUnit{id: id}, profile_hash) do
    "review-quorum:#{id}:#{String.slice(profile_hash, 0, 16)}"
  end

  defp quorum_mode(profile) do
    case value(profile, "quorum_mode") do
      mode when mode in @quorum_modes -> mode
      _other -> quorum_mode_from_required_count(value(profile, "required_decisions"))
    end
  end

  defp quorum_mode_from_required_count(count) when is_integer(count) and count > 1, do: "m_of_n"
  defp quorum_mode_from_required_count(_count), do: "single_decision"

  defp required_decision_count(profile, "two_person"), do: max(required_count(profile, 2), 2)
  defp required_decision_count(profile, "single_decision"), do: max(required_count(profile, 1), 1)
  defp required_decision_count(profile, _mode), do: max(required_count(profile, 1), 1)

  defp minimum_distinct_actors(profile, "single_decision", _count),
    do: max(distinct_actor_count(profile, 1), 1)

  defp minimum_distinct_actors(profile, "two_person", _count),
    do: max(distinct_actor_count(profile, 2), 2)

  defp minimum_distinct_actors(profile, _mode, required_decision_count),
    do: max(distinct_actor_count(profile, required_decision_count), 1)

  defp required_count(profile, default),
    do: integer_value(value(profile, "required_decisions"), default)

  defp distinct_actor_count(profile, default),
    do: integer_value(value(profile, "minimum_distinct_actors"), default)

  defp integer_value(value, default) do
    case value do
      value when is_integer(value) and value >= 0 -> value
      value when is_binary(value) -> parse_integer(value, default)
      _other -> default
    end
  end

  defp parse_integer(value, default) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> integer
      _other -> default
    end
  end

  defp eligible_actor_refs(profile, reviewer_actor) do
    case string_list(value(profile, "eligible_actor_refs")) do
      [] -> reviewer_actor_ref(reviewer_actor)
      actor_refs -> actor_refs
    end
  end

  defp reviewer_actor_ref(%{} = reviewer_actor) do
    case value(normalize_profile(reviewer_actor), "ref") ||
           value(normalize_profile(reviewer_actor), "id") do
      value when is_binary(value) and value != "" -> [value]
      _other -> []
    end
  end

  defp reviewer_actor_ref(_reviewer_actor), do: []

  defp quorum_state(status) when status in [:accepted, :rejected, :waived, :escalated],
    do: Atom.to_string(status)

  defp quorum_state(_status), do: "pending"

  defp quorum_result(:accepted), do: "met"
  defp quorum_result(:rejected), do: "rejected"
  defp quorum_result(:waived), do: "waived"
  defp quorum_result(:escalated), do: "escalated"
  defp quorum_result(_status), do: "pending"

  defp decision_idempotency_key(%ReviewUnit{id: id}, quorum_mode, profile_hash) do
    "review-decision:#{id}:#{quorum_mode}:#{String.slice(profile_hash, 0, 16)}"
  end

  defp enum_value(value, allowed, default) do
    if value in allowed, do: value, else: default
  end

  defp boolean_value(value, _default) when is_boolean(value), do: value
  defp boolean_value(_value, default), do: default

  defp string_list(values) when is_list(values) do
    values
    |> Enum.map(&optional_string/1)
    |> Enum.reject(&is_nil/1)
  end

  defp string_list(value) when is_binary(value) and value != "", do: [value]
  defp string_list(_value), do: []

  defp list_value(value) when is_list(value), do: value
  defp list_value(_value), do: []

  defp optional_string(value) when is_binary(value) and value != "", do: value
  defp optional_string(value) when is_atom(value), do: Atom.to_string(value)
  defp optional_string(_value), do: nil

  defp datetime_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp datetime_value(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)
  defp datetime_value(_datetime), do: nil

  defp value(map, key), do: Map.get(map, key)

  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)

  defp normalize_value(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} -> {to_string(key), normalize_value(nested_value)} end)
  end

  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value), do: value
end
