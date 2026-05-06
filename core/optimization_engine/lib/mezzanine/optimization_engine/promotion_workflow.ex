defmodule Mezzanine.OptimizationEngine.PromotionWorkflow.Decision do
  @moduledoc "Promotion or rollback decision."

  @type t :: %__MODULE__{
          decision_class: :promote | :blocked,
          candidate_ref: String.t(),
          promotion_ref: String.t() | nil,
          rollback_ref: String.t() | nil,
          blocked_gate_refs: [String.t()],
          trace_refs: [String.t()]
        }

  @enforce_keys [:decision_class, :candidate_ref, :blocked_gate_refs, :trace_refs]
  defstruct [
    :decision_class,
    :candidate_ref,
    :promotion_ref,
    :rollback_ref,
    :blocked_gate_refs,
    :trace_refs
  ]
end

defmodule Mezzanine.OptimizationEngine.PromotionWorkflow do
  @moduledoc """
  Promotion workflow gate evaluation for GEPA candidates.
  """

  alias GEPAFramework.Value
  alias Mezzanine.OptimizationEngine.PromotionWorkflow.Decision

  @gate_order [
    {:eval_gate, "gate:eval"},
    {:replay_gate, "gate:replay"},
    {:guardrail_gate, "gate:guardrail"},
    {:budget_gate, "gate:budget"},
    {:shadow_gate, "gate:shadow"},
    {:canary_gate, "gate:canary"},
    {:human_approval_gate, "gate:human_approval"}
  ]

  @spec evaluate(map()) :: {:ok, Decision.t()} | {:error, Decision.t()}
  def evaluate(attrs) when is_map(attrs) do
    blocked_gate_refs = blocked_gate_refs(attrs)

    if blocked_gate_refs == [] do
      {:ok,
       %Decision{
         decision_class: :promote,
         candidate_ref: Value.get(attrs, :candidate_ref),
         promotion_ref: Value.get(attrs, :promotion_ref),
         blocked_gate_refs: [],
         trace_refs: Value.string_list(Value.get(attrs, :trace_refs, []))
       }}
    else
      {:error,
       %Decision{
         decision_class: :blocked,
         candidate_ref: Value.get(attrs, :candidate_ref),
         rollback_ref: Value.get(attrs, :rollback_ref),
         blocked_gate_refs: blocked_gate_refs,
         trace_refs: Value.string_list(Value.get(attrs, :trace_refs, []))
       }}
    end
  end

  def evaluate(_attrs) do
    {:error,
     %Decision{
       decision_class: :blocked,
       candidate_ref: "candidate:invalid",
       rollback_ref: "rollback:invalid",
       blocked_gate_refs: ["gate:invalid"],
       trace_refs: []
     }}
  end

  defp blocked_gate_refs(attrs) do
    gate_failure =
      Enum.find_value(@gate_order, fn {field, gate_ref} ->
        if Value.get(attrs, field) == :pass, do: nil, else: gate_ref
      end)

    cond do
      is_binary(gate_failure) -> [gate_failure]
      Value.get(attrs, :score_delta, 0.0) < 0 -> ["gate:regression"]
      true -> []
    end
  end
end
