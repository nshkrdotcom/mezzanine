defmodule Mezzanine.Policy.ReviewRules do
  @moduledoc """
  Typed review-rules compiler.
  """

  alias Mezzanine.Policy.Helpers

  @type t :: %{required: boolean(), required_decisions: non_neg_integer(), gates: [String.t()]}

  @spec from_config(map()) :: {:ok, t()}
  def from_config(config) do
    review = Helpers.section(config, :review)
    required = Helpers.value(review, :required, false)

    {:ok,
     %{
       required: required,
       required_decisions:
         Helpers.value(review, :required_decisions, if(required, do: 1, else: 0)),
       gates: Helpers.string_list(Helpers.value(review, :gates, []))
     }}
  end
end
