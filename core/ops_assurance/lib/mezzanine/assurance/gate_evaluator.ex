defmodule Mezzanine.Assurance.GateEvaluator do
  @moduledoc """
  Pure gate-status evaluation for review units and open escalations.
  """

  @spec evaluate([struct()], [struct()]) :: map()
  def evaluate(review_units, escalations \\ [])
      when is_list(review_units) and is_list(escalations) do
    counts = Enum.frequencies_by(review_units, & &1.status)
    open_escalation_count = Enum.count(escalations, &(&1.status == :open))

    status =
      cond do
        open_escalation_count > 0 -> :escalated
        Map.get(counts, :rejected, 0) > 0 -> :rejected
        Map.get(counts, :pending, 0) > 0 or Map.get(counts, :in_review, 0) > 0 -> :pending
        review_units == [] -> :clear
        true -> :approved
      end

    %{
      status: status,
      pending_count: Map.get(counts, :pending, 0) + Map.get(counts, :in_review, 0),
      accepted_count: Map.get(counts, :accepted, 0),
      waived_count: Map.get(counts, :waived, 0),
      rejected_count: Map.get(counts, :rejected, 0),
      escalated_count: open_escalation_count,
      release_ready?: status in [:approved, :clear]
    }
  end
end
