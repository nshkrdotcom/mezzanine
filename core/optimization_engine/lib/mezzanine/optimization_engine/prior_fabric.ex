defmodule Mezzanine.OptimizationEngine.PriorFabric.Receipt do
  @moduledoc "Ref-only prior-fabric binding receipt for governed optimization."

  @enforce_keys [
    :receipt_ref,
    :fixture_refs,
    :run_ref,
    :tenant_ref,
    :optimization_target_ref,
    :memory_ref_set,
    :prompt_ref_set,
    :context_budget_ref,
    :guardrail_ref_set,
    :eval_ref_set,
    :replay_ref_set,
    :drift_ref_set,
    :cost_budget_ref_set,
    :trace_ref_set,
    :gate_evidence_refs,
    :promotion_ref,
    :rollback_ref,
    :persistence_refs,
    :local_restart_safe_profile_ref,
    :status
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.OptimizationEngine.PriorFabric do
  @moduledoc """
  Binds inherited memory, prompt, guardrail, eval, replay, cost, and persistence refs.
  """

  alias GEPAFramework.Value
  alias Mezzanine.OptimizationEngine.PriorFabric.Receipt

  @fixture_refs ["AOC-032", "AOC-033", "PERSIST-AOC-004", "PERSIST-AOC-005"]
  @required_strings [
    :run_ref,
    :tenant_ref,
    :optimization_target_ref,
    :context_budget_ref,
    :promotion_ref,
    :rollback_ref,
    :local_restart_safe_profile_ref
  ]
  @required_lists [
    :memory_ref_set,
    :prompt_ref_set,
    :guardrail_ref_set,
    :eval_ref_set,
    :replay_ref_set,
    :drift_ref_set,
    :cost_budget_ref_set,
    :trace_ref_set,
    :gate_evidence_refs
  ]
  @required_persistence_refs [
    :profile_ref,
    :selected_tier_ref,
    :store_ref,
    :retention_ref,
    :debug_tap_posture_ref,
    :restart_safety_ref
  ]
  @raw_fields [
    :api_key,
    :auth_header,
    :credential_body,
    :memory_body,
    :model_output,
    :operator_private_payload,
    :provider_payload,
    :raw_model_output,
    :raw_payload,
    :raw_prompt,
    :secret,
    :token,
    "api_key",
    "auth_header",
    "credential_body",
    "memory_body",
    "model_output",
    "operator_private_payload",
    "provider_payload",
    "raw_model_output",
    "raw_payload",
    "raw_prompt",
    "secret",
    "token"
  ]

  @spec bind(map()) :: {:ok, Receipt.t()} | {:error, term()}
  def bind(attrs) when is_map(attrs) do
    with :ok <- reject_raw(attrs),
         :ok <- required_strings(attrs),
         :ok <- required_lists(attrs),
         {:ok, persistence_refs} <- persistence_refs(attrs) do
      {:ok,
       %Receipt{
         receipt_ref: "prior-fabric://optimization/" <> Value.get(attrs, :run_ref),
         fixture_refs: @fixture_refs,
         run_ref: Value.get(attrs, :run_ref),
         tenant_ref: Value.get(attrs, :tenant_ref),
         optimization_target_ref: Value.get(attrs, :optimization_target_ref),
         memory_ref_set: string_list(attrs, :memory_ref_set),
         prompt_ref_set: string_list(attrs, :prompt_ref_set),
         context_budget_ref: Value.get(attrs, :context_budget_ref),
         guardrail_ref_set: string_list(attrs, :guardrail_ref_set),
         eval_ref_set: string_list(attrs, :eval_ref_set),
         replay_ref_set: string_list(attrs, :replay_ref_set),
         drift_ref_set: string_list(attrs, :drift_ref_set),
         cost_budget_ref_set: string_list(attrs, :cost_budget_ref_set),
         trace_ref_set: string_list(attrs, :trace_ref_set),
         gate_evidence_refs: string_list(attrs, :gate_evidence_refs),
         promotion_ref: Value.get(attrs, :promotion_ref),
         rollback_ref: Value.get(attrs, :rollback_ref),
         persistence_refs: persistence_refs,
         local_restart_safe_profile_ref: Value.get(attrs, :local_restart_safe_profile_ref),
         status: :pass
       }}
    end
  end

  def bind(_attrs), do: {:error, :invalid_prior_fabric_binding}

  defp required_strings(attrs) do
    Enum.reduce_while(@required_strings, :ok, fn field, :ok ->
      if present_string?(Value.get(attrs, field)) do
        {:cont, :ok}
      else
        {:halt, {:error, {:missing_required_ref, field}}}
      end
    end)
  end

  defp required_lists(attrs) do
    Enum.reduce_while(@required_lists, :ok, fn field, :ok ->
      if non_empty_string_list?(Value.get(attrs, field)) do
        {:cont, :ok}
      else
        {:halt, {:error, {:missing_required_refs, field}}}
      end
    end)
  end

  defp persistence_refs(attrs) do
    refs = Value.get(attrs, :persistence_refs, %{})

    if is_map(refs) and required_persistence_refs_present?(refs) do
      {:ok,
       %{
         profile_ref: Value.get(refs, :profile_ref),
         selected_tier_ref: Value.get(refs, :selected_tier_ref),
         store_ref: Value.get(refs, :store_ref),
         retention_ref: Value.get(refs, :retention_ref),
         debug_tap_posture_ref: Value.get(refs, :debug_tap_posture_ref),
         restart_safety_ref: Value.get(refs, :restart_safety_ref)
       }}
    else
      {:error, {:missing_required_refs, :persistence_refs}}
    end
  end

  defp required_persistence_refs_present?(refs) do
    Enum.all?(@required_persistence_refs, &present_string?(Value.get(refs, &1)))
  end

  defp string_list(attrs, field), do: Value.get(attrs, field, []) |> Value.string_list()

  defp non_empty_string_list?(values), do: Value.string_list(values) == values and values != []
  defp present_string?(value), do: is_binary(value) and String.trim(value) != ""

  defp reject_raw(attrs) do
    case raw_field(attrs) do
      nil -> :ok
      field -> {:error, {:forbidden_raw_field, field}}
    end
  end

  defp raw_field(%_struct{} = value), do: value |> Map.from_struct() |> raw_field()

  defp raw_field(value) when is_map(value) do
    Enum.find_value(value, fn {key, nested} ->
      if key in @raw_fields, do: key, else: raw_field(nested)
    end)
  end

  defp raw_field(values) when is_list(values), do: Enum.find_value(values, &raw_field/1)
  defp raw_field(_value), do: nil
end
