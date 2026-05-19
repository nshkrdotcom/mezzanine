defmodule Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.ReceiptNormalizer do
  @moduledoc false

  alias Mezzanine.IntegrationBridge.ProviderAdapters.CodexCli.Support
  alias Mezzanine.IntegrationBridge.ProviderAuthorityAdmission

  import Support

  @spec action_status(term()) :: :succeeded | :cancelled | :timed_out | :failed
  def action_status("completed"), do: :succeeded
  def action_status("stopped"), do: :succeeded
  def action_status("cancelled"), do: :cancelled
  def action_status("canceled"), do: :cancelled
  def action_status("timeout"), do: :timed_out
  def action_status(_status), do: :failed

  @spec operation_receipt(
          map(),
          String.t(),
          String.t() | nil,
          String.t() | nil,
          term(),
          list(),
          term()
        ) :: map()
  def operation_receipt(
        attrs,
        capability_id,
        lower_request_ref,
        lower_receipt_ref,
        status,
        artifact_refs,
        authority_handoff
      ) do
    authority_fields = ProviderAuthorityAdmission.result_fields(authority_handoff)
    run_ref = map_value(attrs, :run_ref)

    %{
      operation_receipt_ref: lower_receipt_ref,
      lower_receipt_ref: lower_receipt_ref,
      lower_request_ref: lower_request_ref,
      lower_runtime_kind: "codex_session",
      status: action_receipt_status_token(status),
      capability_id: capability_id,
      action_id: capability_id,
      effect_request_ref: lower_request_ref,
      connector_ref: codex_connector_ref(),
      connector_manifest_ref: codex_connector_manifest_ref(),
      connector_binding_ref: Map.get(authority_fields, :connector_binding_ref),
      credential_lease_ref: Map.get(authority_fields, :credential_lease_ref),
      capability_negotiation_ref: capability_negotiation_ref(lower_request_ref),
      authority_ref: Map.get(authority_fields, :authority_packet_ref),
      authority_handoff_ref: Map.get(authority_fields, :authority_handoff_ref),
      trace_id: map_value(attrs, :trace_id),
      tenant_ref: map_value(attrs, :tenant_ref) || map_value(attrs, :tenant_id),
      subject_ref: map_value(attrs, :subject_ref),
      run_ref: run_ref,
      evidence_profile_ref: codex_evidence_profile_ref(run_ref),
      artifact_refs: artifact_refs
    }
    |> compact_map()
  end
end
