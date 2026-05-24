defmodule Mezzanine.ContextPacketEngine.Admitter do
  @moduledoc """
  Behaviour for admitting a compiled Context ABI packet into Mezzanine truth.
  """

  alias Mezzanine.ContextPacketEngine.{AdmissionReceipt, AdmissionRequest}
  alias OuterBrain.ContextABI.{ContextPacket, Failure}

  @type admission_request :: AdmissionRequest.t() | map()
  @type admission_receipt :: AdmissionReceipt.t()

  @callback admit(ContextPacket.t(), admission_request(), keyword()) ::
              {:ok, admission_receipt()} | {:error, Failure.t()}
end
