defmodule MezzanineOperatorEngine do
  @moduledoc """
  Operator-control package entrypoint for the Mezzanine rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.Operator.Store,
      Mezzanine.ControlRoom.ForensicReplay,
      Mezzanine.ControlRoom.IncidentBundle,
      Mezzanine.ControlRoom.IncidentExportBundle,
      Mezzanine.OperatorCommands
    ]
  end
end
