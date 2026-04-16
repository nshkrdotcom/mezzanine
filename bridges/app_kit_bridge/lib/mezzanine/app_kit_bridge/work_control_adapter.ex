defmodule Mezzanine.AppKitBridge.WorkControlAdapter do
  @moduledoc """
  `AppKit.WorkControl` backend implemented against Mezzanine work and planning.
  """

  @behaviour AppKit.Core.Backends.WorkBackend

  alias Mezzanine.AppKitBridge.WorkControlService

  @doc """
  Starts a governed run through the extracted work-control service layer.
  """
  @impl true
  def start_run(domain_call, opts) when is_map(domain_call) do
    WorkControlService.start_run(domain_call, opts)
  end
end
