defmodule Mezzanine.SourceEngine.SourceBinding do
  @moduledoc """
  Installation-scoped provider binding used by source coordinators.
  """

  @enforce_keys [:source_binding_id, :installation_id, :provider, :connection_ref]
  defstruct [
    :source_binding_id,
    :installation_id,
    :provider,
    :connection_ref,
    candidate_filters: %{},
    state_mapping: %{},
    poll_cadence_ms: nil,
    webhook_route_ref: nil,
    contract_version: "Mezzanine.SourceBinding.v1"
  ]

  @type t :: %__MODULE__{
          contract_version: String.t(),
          source_binding_id: String.t(),
          installation_id: String.t(),
          provider: String.t(),
          connection_ref: String.t(),
          candidate_filters: map(),
          state_mapping: map(),
          poll_cadence_ms: pos_integer() | nil,
          webhook_route_ref: String.t() | nil
        }
end
