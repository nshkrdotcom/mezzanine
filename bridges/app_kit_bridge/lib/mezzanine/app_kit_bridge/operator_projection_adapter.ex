defmodule Mezzanine.AppKitBridge.OperatorProjectionAdapter do
  @moduledoc """
  `AppKit.OperatorSurface` backend implemented against Mezzanine audit and
  assurance services.
  """

  @behaviour AppKit.Core.Backends.OperatorBackend

  alias AppKit.Core.RunRef
  alias Mezzanine.AppKitBridge.{OperatorActionService, OperatorQueryService}

  @doc """
  Projects operator-facing run status through the extracted operator query
  service layer.
  """
  @impl true
  def run_status(%RunRef{} = run_ref, attrs, opts) when is_map(attrs) do
    OperatorQueryService.run_status(run_ref, attrs, opts)
  end

  @doc """
  Records a review decision through the extracted operator action service layer.
  """
  @impl true
  def review_run(%RunRef{} = run_ref, evidence_attrs, opts) when is_map(evidence_attrs) do
    OperatorActionService.review_run(run_ref, evidence_attrs, opts)
  end
end
