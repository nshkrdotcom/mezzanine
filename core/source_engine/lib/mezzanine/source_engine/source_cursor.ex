defmodule Mezzanine.SourceEngine.SourceCursor do
  @moduledoc """
  Poll/webhook checkpoint contract for source coordinators.
  """

  @enforce_keys [:source_binding_id]
  defstruct [
    :source_binding_id,
    :cursor,
    :last_polled_at,
    :last_successful_event_id,
    :failure_class,
    refresh_requested?: false,
    contract_version: "Mezzanine.SourceCursor.v1"
  ]

  @type t :: %__MODULE__{
          contract_version: String.t(),
          source_binding_id: String.t(),
          cursor: String.t() | nil,
          last_polled_at: DateTime.t() | nil,
          last_successful_event_id: String.t() | nil,
          failure_class: String.t() | nil,
          refresh_requested?: boolean()
        }
end
