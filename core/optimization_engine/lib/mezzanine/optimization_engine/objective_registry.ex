defmodule Mezzanine.OptimizationEngine.ObjectiveRegistry.Objective do
  @moduledoc "Governed objective descriptor."

  @type t :: %__MODULE__{
          objective_type: atom(),
          objective_ref: String.t(),
          eval_foundation_ref: String.t(),
          replay_foundation_ref: String.t(),
          cost_foundation_ref: String.t()
        }

  @enforce_keys [
    :objective_type,
    :objective_ref,
    :eval_foundation_ref,
    :replay_foundation_ref,
    :cost_foundation_ref
  ]
  defstruct @enforce_keys
end

defmodule Mezzanine.OptimizationEngine.ObjectiveRegistry do
  @moduledoc """
  Objective registry for governed GEPA runs.
  """

  alias Mezzanine.OptimizationEngine.ObjectiveRegistry.Objective

  @supported_types [
    :exact,
    :semantic,
    :faithfulness,
    :retrieval,
    :tool_success,
    :latency,
    :cost,
    :safety,
    :human_preference,
    :verifier,
    :constrained,
    :pareto
  ]

  @spec supported_types() :: [atom()]
  def supported_types, do: @supported_types

  @spec fetch(atom()) :: {:ok, Objective.t()} | {:error, term()}
  def fetch(type) when type in @supported_types do
    name = Atom.to_string(type)

    {:ok,
     %Objective{
       objective_type: type,
       objective_ref: "objective:" <> name,
       eval_foundation_ref: "eval_foundation:" <> name,
       replay_foundation_ref: "replay_foundation:" <> name,
       cost_foundation_ref: "cost_foundation:" <> name
     }}
  end

  def fetch(type), do: {:error, {:unknown_objective_type, type}}
end
