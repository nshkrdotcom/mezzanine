defmodule Mezzanine.AIExecution.RuntimeDeps do
  @moduledoc """
  Explicit runtime dependency selection for Mezzanine AI execution.
  """

  defstruct context_admitter: nil,
            renderer: nil,
            router_adapter: nil,
            optimizer_adapter: nil,
            model_invoker: nil,
            clock: nil,
            id_generator: nil

  @type t :: %__MODULE__{
          context_admitter: module() | nil,
          renderer: module() | nil,
          router_adapter: module() | nil,
          optimizer_adapter: module() | nil,
          model_invoker: module() | nil,
          clock: module() | nil,
          id_generator: module() | nil
        }

  @spec new!(t() | map() | keyword()) :: t()
  def new!(%__MODULE__{} = deps), do: deps
  def new!(attrs) when is_list(attrs), do: attrs |> Map.new() |> new!()

  def new!(attrs) when is_map(attrs) do
    %__MODULE__{
      context_admitter: value(attrs, :context_admitter),
      renderer: value(attrs, :renderer),
      router_adapter: value(attrs, :router_adapter),
      optimizer_adapter: value(attrs, :optimizer_adapter),
      model_invoker: value(attrs, :model_invoker),
      clock: value(attrs, :clock),
      id_generator: value(attrs, :id_generator)
    }
  end

  defp value(attrs, field), do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))
end
