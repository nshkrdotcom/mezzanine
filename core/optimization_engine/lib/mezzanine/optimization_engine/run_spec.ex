defmodule Mezzanine.OptimizationEngine.RunSpec do
  @moduledoc """
  Ref-only governed optimization run spec.
  """

  alias Mezzanine.AIExecution.OptimizerAdapter

  @type t :: %__MODULE__{
          run_ref: String.t(),
          tenant_ref: String.t(),
          authority_ref: String.t(),
          target_ref: String.t(),
          framework_run_ref: String.t(),
          checkpoint_ref: String.t(),
          budget_ref: String.t(),
          eval_suite_ref: String.t(),
          replay_bundle_ref: String.t(),
          trace_ref: String.t(),
          memory_ref_set: [String.t()],
          prompt_ref_set: [String.t()],
          context_budget_ref: String.t(),
          guardrail_ref_set: [String.t()],
          cost_budget_ref_set: [String.t()],
          drift_ref_set: [String.t()],
          persistence_ref_set: [String.t()],
          promotion_ref_set: [String.t()],
          rollback_ref_set: [String.t()]
        }

  @required_fields [
    :run_ref,
    :tenant_ref,
    :authority_ref,
    :target_ref,
    :framework_run_ref,
    :checkpoint_ref,
    :budget_ref,
    :eval_suite_ref,
    :replay_bundle_ref,
    :trace_ref,
    :context_budget_ref
  ]
  @required_lists [
    :memory_ref_set,
    :prompt_ref_set,
    :guardrail_ref_set,
    :cost_budget_ref_set,
    :drift_ref_set,
    :persistence_ref_set,
    :promotion_ref_set,
    :rollback_ref_set
  ]
  @enforce_keys @required_fields ++ @required_lists
  defstruct @enforce_keys

  @forbidden_raw_fields [
    :prompt,
    "prompt",
    :provider_payload,
    "provider_payload",
    :model_output,
    "model_output",
    :memory_body,
    "memory_body",
    :secret,
    "secret",
    :workflow_history,
    "workflow_history"
  ]

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- reject_raw_fields(attrs),
         :ok <- required_strings(attrs),
         :ok <- required_ref_lists(attrs) do
      {:ok,
       %__MODULE__{
         run_ref: value(attrs, :run_ref),
         tenant_ref: value(attrs, :tenant_ref),
         authority_ref: value(attrs, :authority_ref),
         target_ref: value(attrs, :target_ref),
         framework_run_ref: value(attrs, :framework_run_ref),
         checkpoint_ref: value(attrs, :checkpoint_ref),
         budget_ref: value(attrs, :budget_ref),
         eval_suite_ref: value(attrs, :eval_suite_ref),
         replay_bundle_ref: value(attrs, :replay_bundle_ref),
         trace_ref: value(attrs, :trace_ref),
         memory_ref_set: string_list(attrs, :memory_ref_set),
         prompt_ref_set: string_list(attrs, :prompt_ref_set),
         context_budget_ref: value(attrs, :context_budget_ref),
         guardrail_ref_set: string_list(attrs, :guardrail_ref_set),
         cost_budget_ref_set: string_list(attrs, :cost_budget_ref_set),
         drift_ref_set: string_list(attrs, :drift_ref_set),
         persistence_ref_set: string_list(attrs, :persistence_ref_set),
         promotion_ref_set: string_list(attrs, :promotion_ref_set),
         rollback_ref_set: string_list(attrs, :rollback_ref_set)
       }}
    end
  end

  def new(_attrs), do: {:error, :invalid_optimization_run_spec}

  @spec new!(map()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, spec} -> spec
      {:error, reason} -> raise ArgumentError, "invalid optimization run spec: #{inspect(reason)}"
    end
  end

  @spec framework_config(t()) :: keyword()
  def framework_config(%__MODULE__{} = spec) do
    [
      runtime_ref: spec.framework_run_ref,
      task: %{
        task_ref: spec.target_ref,
        dataset_ref: spec.eval_suite_ref,
        memory_ref_set: spec.memory_ref_set,
        context_budget_ref: spec.context_budget_ref
      },
      components: component_specs(spec),
      evaluator: %{
        evaluator_ref: spec.eval_suite_ref,
        objective_refs: ["objective:exact"],
        replay_bundle_ref: spec.replay_bundle_ref,
        guardrail_ref_set: spec.guardrail_ref_set
      },
      proposer: %{
        proposer_ref: "proposer:governed:deterministic",
        strategy: :deterministic_reflection
      },
      merge: %{
        merge_ref: "merge:governed:disabled",
        strategy: :disabled
      },
      budget: %{budget_ref_set: spec.cost_budget_ref_set},
      drift: %{drift_ref_set: spec.drift_ref_set},
      tracing: %{trace_refs: [spec.trace_ref]},
      persistence: %{profile: :memory_ephemeral, ref_set: spec.persistence_ref_set},
      promotion: %{
        promotion_ref_set: spec.promotion_ref_set,
        rollback_ref_set: spec.rollback_ref_set
      }
    ]
  end

  @spec optimizer_request(t()) :: OptimizerAdapter.optimization_request()
  def optimizer_request(%__MODULE__{} = spec) do
    %{
      tenant_ref: spec.tenant_ref,
      run_ref: spec.framework_run_ref,
      objective_ref: spec.eval_suite_ref,
      candidate_source_refs: candidate_source_refs(spec),
      promotion_policy_ref: first_ref(spec.promotion_ref_set),
      trace_ref: spec.trace_ref,
      context_packet_ref: spec.context_budget_ref,
      route_decision_ref: spec.target_ref,
      eval_refs: [spec.eval_suite_ref],
      cost_refs: spec.cost_budget_ref_set,
      promotion_ref: first_ref(spec.promotion_ref_set),
      rollback_ref: first_ref(spec.rollback_ref_set),
      optimization_target_ref: spec.target_ref
    }
  end

  defp component_specs(spec) do
    spec.prompt_ref_set
    |> Enum.with_index(1)
    |> Enum.map(fn {prompt_ref, index} ->
      %{
        component_ref: "component:" <> Integer.to_string(index) <> ":" <> spec.target_ref,
        kind: :instruction,
        content_ref: prompt_ref
      }
    end)
  end

  @spec candidate_source_refs(t()) :: [String.t()]
  defp candidate_source_refs(spec) do
    [
      spec.memory_ref_set,
      spec.prompt_ref_set,
      [spec.context_budget_ref],
      spec.guardrail_ref_set,
      spec.drift_ref_set
    ]
    |> List.flatten()
    |> Enum.uniq()
  end

  @spec first_ref([String.t(), ...] | [String.t()]) :: String.t()
  defp first_ref([ref | _rest]) when is_binary(ref), do: ref

  defp required_strings(attrs) do
    missing =
      @required_fields
      |> Enum.reject(&(is_binary(value(attrs, &1)) and value(attrs, &1) != ""))

    case missing do
      [] -> :ok
      [field | _rest] -> {:error, {:missing_required_ref, field}}
    end
  end

  defp string_list(attrs, field), do: attrs |> value(field, []) |> normalize_string_list()

  defp required_ref_lists(attrs) do
    missing =
      @required_lists
      |> Enum.reject(&non_empty_string_list?(value(attrs, &1)))

    case missing do
      [] -> :ok
      [field | _rest] -> {:error, {:missing_required_refs, field}}
    end
  end

  defp non_empty_string_list?(value), do: normalize_string_list(value) == value and value != []

  defp reject_raw_fields(attrs) do
    case forbidden_field(attrs) do
      nil -> :ok
      field -> {:error, {:forbidden_raw_field, field}}
    end
  end

  defp forbidden_field(input) when is_map(input) do
    Enum.find_value(input, fn {key, value} ->
      if key in @forbidden_raw_fields, do: key, else: forbidden_field(value)
    end)
  end

  defp forbidden_field(input) when is_list(input), do: Enum.find_value(input, &forbidden_field/1)
  defp forbidden_field(_input), do: nil

  defp value(attrs, field, default \\ nil),
    do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field)) || default

  defp normalize_string_list(values) when is_list(values) do
    Enum.filter(values, &(is_binary(&1) and &1 != ""))
  end

  defp normalize_string_list(_values), do: []
end
