defmodule Mezzanine.OptimizationEngine.PromotionWorkflow.Decision do
  @moduledoc "Promotion or rollback decision."

  @type t :: %__MODULE__{
          decision_class: :promote | :blocked,
          candidate_ref: String.t(),
          promotion_ref: String.t() | nil,
          rollback_ref: String.t() | nil,
          blocked_gate_refs: [String.t()],
          trace_refs: [String.t()],
          gate_evidence_refs: [String.t()],
          operator_evidence_refs: [String.t()]
        }

  @enforce_keys [
    :decision_class,
    :candidate_ref,
    :blocked_gate_refs,
    :trace_refs,
    :gate_evidence_refs,
    :operator_evidence_refs
  ]
  defstruct [
    :decision_class,
    :candidate_ref,
    :promotion_ref,
    :rollback_ref,
    :blocked_gate_refs,
    :trace_refs,
    :gate_evidence_refs,
    :operator_evidence_refs
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
  @required_gate_evidence_refs [
    "gate-evidence://eval",
    "gate-evidence://replay",
    "gate-evidence://guardrail",
    "gate-evidence://budget"
  ]

  @spec evaluate(map()) :: {:ok, Decision.t()} | {:error, Decision.t()}
  def evaluate(attrs) when is_map(attrs) do
    blocked_gate_refs = blocked_gate_refs(attrs)
    gate_evidence_refs = Value.string_list(Value.get(attrs, :gate_evidence_refs, []))
    operator_evidence_refs = Value.string_list(Value.get(attrs, :operator_evidence_refs, []))

    if blocked_gate_refs == [] do
      {:ok,
       %Decision{
         decision_class: :promote,
         candidate_ref: Value.get(attrs, :candidate_ref),
         promotion_ref: Value.get(attrs, :promotion_ref),
         blocked_gate_refs: [],
         trace_refs: Value.string_list(Value.get(attrs, :trace_refs, [])),
         gate_evidence_refs: gate_evidence_refs,
         operator_evidence_refs: operator_evidence_refs
       }}
    else
      {:error,
       %Decision{
         decision_class: :blocked,
         candidate_ref: Value.get(attrs, :candidate_ref),
         rollback_ref: rollback_ref(attrs, blocked_gate_refs),
         blocked_gate_refs: blocked_gate_refs,
         trace_refs: Value.string_list(Value.get(attrs, :trace_refs, [])),
         gate_evidence_refs: gate_evidence_refs,
         operator_evidence_refs: operator_evidence_refs
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
       trace_refs: [],
       gate_evidence_refs: [],
       operator_evidence_refs: []
     }}
  end

  defp blocked_gate_refs(attrs) do
    gate_failure =
      Enum.find_value(@gate_order, fn {field, gate_ref} ->
        if Value.get(attrs, field) == :pass, do: nil, else: gate_ref
      end)

    cond do
      is_binary(gate_failure) -> [gate_failure]
      missing_evidence?(attrs) -> ["gate:evidence"]
      Value.get(attrs, :score_delta, 0.0) < 0 -> ["gate:regression"]
      true -> []
    end
  end

  defp missing_evidence?(attrs) do
    gate_evidence_refs = Value.string_list(Value.get(attrs, :gate_evidence_refs, []))
    operator_evidence_refs = Value.string_list(Value.get(attrs, :operator_evidence_refs, []))

    not Enum.all?(@required_gate_evidence_refs, &(&1 in gate_evidence_refs)) or
      operator_evidence_refs == []
  end

  defp rollback_ref(_attrs, ["gate:evidence"]), do: "rollback:evidence-missing"
  defp rollback_ref(attrs, _blocked_gate_refs), do: Value.get(attrs, :rollback_ref)
end
