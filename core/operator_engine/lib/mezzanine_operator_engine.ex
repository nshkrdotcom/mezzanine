defmodule MezzanineOperatorEngine do
  @moduledoc """
  Operator-control package entrypoint for the Mezzanine rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.OperatorCommands,
      Mezzanine.ExecutionCancelWorker
    ]
  end
end
