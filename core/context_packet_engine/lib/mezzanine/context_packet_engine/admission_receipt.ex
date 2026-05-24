defmodule Mezzanine.ContextPacketEngine.AdmissionReceipt do
  @moduledoc """
  Durable, product-projectable packet admission receipt.
  """

  alias GroundPlane.Boundary.Codec
  alias Mezzanine.ContextPacketEngine.AdmissionRequest
  alias OuterBrain.ContextABI.{ContextPacket, Failure}

  @statuses [:admitted, :rejected, :duplicate]

  @enforce_keys [
    :receipt_ref,
    :context_packet_ref,
    :workflow_ref,
    :tenant_ref,
    :authority_ref,
    :packet_hash,
    :status,
    :idempotency_key,
    :trace_ref,
    :joins
  ]

  defstruct @enforce_keys ++ [failure: nil]

  @type status :: :admitted | :rejected | :duplicate

  @type t :: %__MODULE__{
          receipt_ref: String.t(),
          context_packet_ref: String.t(),
          workflow_ref: String.t(),
          tenant_ref: String.t(),
          authority_ref: String.t(),
          packet_hash: String.t(),
          status: status(),
          idempotency_key: String.t(),
          trace_ref: String.t(),
          joins: map(),
          failure: Failure.t() | nil
        }

  @spec admitted(ContextPacket.t(), AdmissionRequest.t()) :: t()
  def admitted(%ContextPacket{} = packet, %AdmissionRequest{} = request) do
    new!(packet, request, :admitted, nil)
  end

  @spec rejected(ContextPacket.t(), AdmissionRequest.t(), Failure.t()) :: t()
  def rejected(%ContextPacket{} = packet, %AdmissionRequest{} = request, %Failure{} = failure) do
    new!(packet, request, :rejected, failure)
  end

  @spec duplicate(t()) :: t()
  def duplicate(%__MODULE__{} = receipt), do: %{receipt | status: :duplicate}

  @spec redacted_projection(t()) :: map()
  def redacted_projection(%__MODULE__{} = receipt) do
    %{
      receipt_ref: receipt.receipt_ref,
      context_packet_ref: receipt.context_packet_ref,
      workflow_ref: receipt.workflow_ref,
      tenant_ref: receipt.tenant_ref,
      authority_ref: receipt.authority_ref,
      packet_hash: receipt.packet_hash,
      status: receipt.status,
      idempotency_key: receipt.idempotency_key,
      trace_ref: receipt.trace_ref,
      joins: receipt.joins,
      failure: safe_failure(receipt.failure)
    }
  end

  defp new!(%ContextPacket{} = packet, %AdmissionRequest{} = request, status, failure)
       when status in @statuses do
    %__MODULE__{
      receipt_ref: receipt_ref(packet, request, status),
      context_packet_ref: packet.context_packet_ref,
      workflow_ref: request.workflow_ref,
      tenant_ref: request.tenant_ref,
      authority_ref: request.authority_ref,
      packet_hash: packet.packet_hash,
      status: status,
      idempotency_key: request.idempotency_key,
      trace_ref: request.trace_ref,
      joins: AdmissionRequest.joins(request),
      failure: failure
    }
  end

  defp receipt_ref(packet, request, status) do
    %{
      schema_ref: "mezzanine.packet_admission_receipt.v1",
      context_packet_ref: packet.context_packet_ref,
      packet_hash: packet.packet_hash,
      workflow_ref: request.workflow_ref,
      authority_ref: request.authority_ref,
      tenant_ref: request.tenant_ref,
      idempotency_key: request.idempotency_key,
      status: Atom.to_string(status),
      trace_ref: request.trace_ref
    }
    |> Codec.digest()
    |> String.replace_prefix("sha256:", "packet-admission://")
  end

  defp safe_failure(nil), do: nil

  defp safe_failure(%Failure{} = failure) do
    %{
      owner: failure.owner,
      reason_code: failure.reason_code,
      safe_message: failure.safe_message,
      retryable?: failure.retryable?,
      trace_ref: failure.trace_ref,
      evidence_refs: failure.evidence_refs
    }
  end
end
