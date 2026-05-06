defmodule Mezzanine.BudgetEnforcementEngine do
  @moduledoc """
  Fail-closed budget enforcement at Phase D loci.
  """

  defmodule BudgetRef do
    @moduledoc "Budget identity and cap policy."
    @enforce_keys [
      :budget_id,
      :scope_key,
      :period_class,
      :hard_cap_amount,
      :soft_cap_amount,
      :override_policy_ref
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            budget_id: String.t(),
            scope_key: map(),
            period_class: atom(),
            hard_cap_amount: non_neg_integer(),
            soft_cap_amount: non_neg_integer(),
            override_policy_ref: String.t()
          }
  end

  defmodule BudgetDecision do
    @moduledoc "Deterministic budget decision."
    @enforce_keys [
      :budget_id,
      :locus,
      :decision_class,
      :requested_units,
      :granted_units,
      :residual_units,
      :operator_actions,
      :reason_ref
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            budget_id: String.t(),
            locus: atom(),
            decision_class: atom(),
            requested_units: non_neg_integer(),
            granted_units: non_neg_integer(),
            residual_units: non_neg_integer(),
            operator_actions: [atom()],
            reason_ref: String.t() | nil
          }
  end

  defmodule Ledger do
    @moduledoc "Memory-default decision ledger."
    @enforce_keys [:tier, :decisions]
    defstruct @enforce_keys

    @type t :: %__MODULE__{tier: atom(), decisions: [BudgetDecision.t()]}
  end

  @period_classes [:per_run, :per_skill, :per_day, :per_tenant, :per_authority]
  @loci [:preflight, :append, :stream, :runtime_admission, :reconciliation]
  @decisions [
    :allow,
    :allow_warn_soft,
    :deny_hard_exhausted,
    :deny_policy,
    :deny_revoked,
    :allow_with_override
  ]
  @operator_actions [:accept_override, :reject_override, :escalate_override, :expire_override]
  @scope_fields [:tenant_ref, :installation_ref, :run_ref, :agent_ref, :skill_ref]
  @max_override_seconds 3_600
  @required_strings [:budget_id, :override_policy_ref]

  @spec budget_ref(map()) :: {:ok, BudgetRef.t()} | {:error, term()}
  def budget_ref(attrs) when is_map(attrs) do
    with :ok <- required_strings(attrs, @required_strings),
         {:ok, scope_key} <- scope_key(attrs),
         {:ok, period_class} <- member(attrs, :period_class, @period_classes),
         {:ok, hard_cap_amount} <- non_negative_integer(attrs, :hard_cap_amount),
         {:ok, soft_cap_amount} <- non_negative_integer(attrs, :soft_cap_amount),
         :ok <- soft_not_above_hard(soft_cap_amount, hard_cap_amount) do
      {:ok,
       %BudgetRef{
         budget_id: fetch!(attrs, :budget_id),
         scope_key: scope_key,
         period_class: period_class,
         hard_cap_amount: hard_cap_amount,
         soft_cap_amount: soft_cap_amount,
         override_policy_ref: fetch!(attrs, :override_policy_ref)
       }}
    end
  end

  def budget_ref(_attrs), do: {:error, :invalid_budget_ref}

  @spec new_ledger(keyword()) :: {:ok, Ledger.t()} | {:error, term()}
  def new_ledger(opts \\ []) when is_list(opts) do
    case Keyword.get(opts, :tier, :memory) do
      :memory -> {:ok, %Ledger{tier: :memory, decisions: []}}
      {:durable, :postgres} -> {:error, :budget_postgres_adapter_not_registered}
      _tier -> {:error, :unknown_budget_ledger_tier}
    end
  end

  @spec enforce(Ledger.t(), BudgetRef.t(), map()) ::
          {:ok, Ledger.t(), BudgetDecision.t()} | {:error, term()}
  def enforce(%Ledger{} = ledger, %BudgetRef{} = budget, attrs) when is_map(attrs) do
    with {:ok, locus} <- member(attrs, :locus, @loci),
         {:ok, requested_units} <- non_negative_integer(attrs, :requested_units) do
      used_units = used_units(ledger, budget.budget_id)
      residual_units = max(budget.hard_cap_amount - used_units, 0)
      decision = decide(budget, locus, requested_units, residual_units, attrs)

      {:ok, %{ledger | decisions: ledger.decisions ++ [decision]}, decision}
    end
  end

  def enforce(%Ledger{}, %BudgetRef{}, _attrs), do: {:error, :invalid_budget_enforcement}

  @spec decision_classes() :: [atom()]
  def decision_classes, do: @decisions

  defp decide(budget, locus, requested_units, residual_units, attrs) do
    cond do
      fetch(attrs, :policy_revoked) == true ->
        decision(
          budget.budget_id,
          locus,
          :deny_revoked,
          requested_units,
          0,
          residual_units,
          attrs
        )

      fetch(attrs, :policy_denied) == true ->
        decision(budget.budget_id, locus, :deny_policy, requested_units, 0, residual_units, attrs)

      requested_units > residual_units and override_allowed?(attrs) ->
        decision(
          budget.budget_id,
          locus,
          :allow_with_override,
          requested_units,
          requested_units,
          0,
          attrs
        )

      requested_units > residual_units ->
        decision(
          budget.budget_id,
          locus,
          :deny_hard_exhausted,
          requested_units,
          0,
          residual_units,
          attrs
        )

      requested_units > residual_units - budget.soft_cap_amount ->
        decision(
          budget.budget_id,
          locus,
          :allow_warn_soft,
          requested_units,
          requested_units,
          residual_units - requested_units,
          attrs
        )

      true ->
        decision(
          budget.budget_id,
          locus,
          :allow,
          requested_units,
          requested_units,
          residual_units - requested_units,
          attrs
        )
    end
  end

  defp decision(
         budget_id,
         locus,
         decision_class,
         requested_units,
         granted_units,
         residual_units,
         attrs
       )
       when decision_class in @decisions do
    %BudgetDecision{
      budget_id: budget_id || fetch(attrs, :budget_id),
      locus: locus,
      decision_class: decision_class,
      requested_units: requested_units,
      granted_units: granted_units,
      residual_units: max(residual_units, 0),
      operator_actions: operator_actions(decision_class),
      reason_ref: fetch(attrs, :reason_ref)
    }
  end

  defp operator_actions(:deny_hard_exhausted), do: @operator_actions
  defp operator_actions(:allow_with_override), do: [:expire_override]
  defp operator_actions(_decision_class), do: []

  defp used_units(%Ledger{} = ledger, budget_id) do
    ledger.decisions
    |> Enum.filter(&(&1.budget_id == budget_id))
    |> Enum.map(& &1.granted_units)
    |> Enum.sum()
  end

  defp override_allowed?(attrs) do
    fetch(attrs, :override_permission_ref) == "permission://budget/override" and
      present_string?(fetch(attrs, :reason_ref)) and
      bounded_duration?(fetch(attrs, :duration_seconds))
  end

  defp bounded_duration?(value),
    do: is_integer(value) and value > 0 and value <= @max_override_seconds

  defp scope_key(attrs) do
    scope =
      Enum.reduce(@scope_fields, %{}, fn field, acc ->
        case fetch(attrs, field) do
          value when is_binary(value) and value != "" -> Map.put(acc, field, value)
          _value -> acc
        end
      end)

    if Map.has_key?(scope, :tenant_ref) and Map.has_key?(scope, :installation_ref) do
      {:ok, scope}
    else
      {:error, :invalid_budget_scope_key}
    end
  end

  defp soft_not_above_hard(soft_cap_amount, hard_cap_amount) do
    if soft_cap_amount <= hard_cap_amount do
      :ok
    else
      {:error, :budget_soft_cap_above_hard_cap}
    end
  end

  defp required_strings(attrs, fields) do
    case Enum.find(fields, &(not present_string?(fetch(attrs, &1)))) do
      nil -> :ok
      field -> {:error, {:missing_budget_ref, field}}
    end
  end

  defp member(attrs, field, allowed) do
    case fetch(attrs, field) do
      value when is_atom(value) -> member_atom(value, allowed, field)
      value when is_binary(value) -> member_string(value, allowed, field)
      _value -> {:error, {:unknown_budget_enum, field}}
    end
  end

  defp member_atom(value, allowed, field) do
    if value in allowed do
      {:ok, value}
    else
      {:error, {:unknown_budget_enum, field}}
    end
  end

  defp member_string(value, allowed, field) do
    case Enum.find(allowed, &(Atom.to_string(&1) == value)) do
      nil -> {:error, {:unknown_budget_enum, field}}
      found -> {:ok, found}
    end
  end

  defp non_negative_integer(attrs, field) do
    case fetch(attrs, field) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _value -> {:error, {:invalid_budget_units, field}}
    end
  end

  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""
  defp fetch!(attrs, field), do: fetch(attrs, field)
  defp fetch(attrs, field), do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))
end
