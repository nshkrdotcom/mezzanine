defmodule Mezzanine.Planner.ReviewIntentBuilder do
  @moduledoc """
  Builds review intents from compiled review policy.
  """

  alias Mezzanine.Policy.TypedConfig
  alias MezzanineOpsModel.Intent.ReviewIntent
  alias MezzanineOpsModel.{PolicyBundle, WorkObject}

  @spec build(WorkObject.t(), PolicyBundle.t()) :: {:ok, [ReviewIntent.t()]} | {:error, term()}
  def build(%WorkObject{} = work, %PolicyBundle{} = bundle) do
    review_rules = TypedConfig.review_rules(bundle)

    if review_rules.required do
      review_rules.gates
      |> default_gate()
      |> build_review_intent(work, review_rules.required_decisions)
    else
      {:ok, []}
    end
  end

  defp default_gate([]), do: "operator"
  defp default_gate([gate | _rest]), do: gate

  defp build_review_intent(gate, %WorkObject{} = work, required_decisions) do
    ReviewIntent.new(%{
      intent_id: "review:" <> work.work_id <> ":" <> to_string(gate),
      program_id: work.program_id,
      work_id: work.work_id,
      gate: gate,
      required_decisions: required_decisions,
      metadata: %{reason: :policy_required}
    })
    |> case do
      {:ok, intent} -> {:ok, [intent]}
      {:error, reason} -> {:error, reason}
    end
  end
end
