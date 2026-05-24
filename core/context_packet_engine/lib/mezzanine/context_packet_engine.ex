defmodule Mezzanine.ContextPacketEngine do
  @moduledoc """
  Public facade for Mezzanine-owned Context ABI packet admission.
  """

  alias Mezzanine.ContextPacketEngine.{
    AdmissionReceipt,
    AdmissionRequest,
    Admitter,
    DefaultAdmitter
  }

  @manifest %{
    package: :mezzanine_context_packet_engine,
    layer: :core,
    status: :nshkr_fugu_phase_5_context_packet_admission,
    owns: [
      :context_packet_admission_receipts,
      :workflow_packet_joins,
      :authority_packet_joins,
      :budget_packet_joins,
      :cost_packet_joins,
      :eval_packet_joins,
      :route_packet_joins,
      :model_call_packet_joins,
      :trace_packet_joins,
      :idempotent_packet_admission,
      :product_safe_projection_sources
    ],
    internal_dependencies: [],
    external_dependencies: [
      :outer_brain_context_abi,
      :citadel_context_authority_contract,
      :ground_plane_contracts
    ]
  }

  @spec manifest() :: map()
  def manifest, do: @manifest

  @spec admission_request_module() :: module()
  def admission_request_module, do: AdmissionRequest

  @spec admission_receipt_module() :: module()
  def admission_receipt_module, do: AdmissionReceipt

  @spec admitter_module() :: module()
  def admitter_module, do: Admitter

  @spec default_admitter_module() :: module()
  def default_admitter_module, do: DefaultAdmitter

  @spec admit(
          OuterBrain.ContextABI.ContextPacket.t(),
          AdmissionRequest.t() | map(),
          keyword()
        ) ::
          {:ok, AdmissionReceipt.t()} | {:error, OuterBrain.ContextABI.Failure.t()}
  def admit(context_packet, admission_request, opts \\ []) do
    admitter = Keyword.get(opts, :admitter, DefaultAdmitter)
    admitter.admit(context_packet, admission_request, Keyword.delete(opts, :admitter))
  end

  @spec redacted_projection(AdmissionReceipt.t()) :: map()
  def redacted_projection(%AdmissionReceipt{} = receipt),
    do: AdmissionReceipt.redacted_projection(receipt)
end
