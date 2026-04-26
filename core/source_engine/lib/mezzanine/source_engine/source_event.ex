defmodule Mezzanine.SourceEngine.SourceEvent do
  @moduledoc """
  Provider-neutral source fact admitted by the Mezzanine source engine.
  """

  @enforce_keys [
    :source_event_id,
    :installation_id,
    :source_binding_id,
    :provider,
    :external_ref,
    :event_kind,
    :provider_revision,
    :payload_schema,
    :payload_hash,
    :idempotency_key,
    :trace_id,
    :causation_id
  ]
  defstruct [
    :source_event_id,
    :installation_id,
    :source_binding_id,
    :provider,
    :external_ref,
    :event_kind,
    :provider_revision,
    :payload_schema,
    :payload_hash,
    :idempotency_key,
    :trace_id,
    :causation_id,
    :occurred_at,
    normalized_payload: %{},
    provider_payload_ref: nil,
    status: :accepted,
    contract_version: "Mezzanine.SourceEvent.v1"
  ]

  @type status :: :accepted | :duplicate | :rejected

  @type t :: %__MODULE__{
          contract_version: String.t(),
          source_event_id: String.t(),
          installation_id: String.t(),
          source_binding_id: String.t(),
          provider: String.t(),
          external_ref: String.t(),
          event_kind: String.t(),
          provider_revision: String.t(),
          payload_schema: String.t(),
          payload_hash: String.t(),
          idempotency_key: String.t(),
          trace_id: String.t(),
          causation_id: String.t(),
          occurred_at: DateTime.t() | nil,
          normalized_payload: map(),
          provider_payload_ref: String.t() | nil,
          status: status()
        }
end
