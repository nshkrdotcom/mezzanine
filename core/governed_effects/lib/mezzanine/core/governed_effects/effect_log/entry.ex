defmodule Mezzanine.Core.GovernedEffects.EffectLog.Entry do
  @moduledoc """
  One immutable governed-effect log entry.
  """

  alias Mezzanine.Core.GovernedEffects.Support

  @fields [
    :sequence,
    :effect_ref,
    :tenant_ref,
    :trace_ref,
    :event_kind,
    :status,
    :payload,
    :parent_evidence_hash,
    :entry_hash,
    :occurred_at
  ]

  @enforce_keys [
    :sequence,
    :effect_ref,
    :tenant_ref,
    :trace_ref,
    :event_kind,
    :payload,
    :entry_hash
  ]
  defstruct @fields

  @type t :: %__MODULE__{}

  @spec fields() :: [atom()]
  def fields, do: @fields

  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = entry), do: Support.boundary_map(entry, @fields)

  @spec hash_material(t()) :: map()
  def hash_material(%__MODULE__{} = entry), do: entry |> to_map() |> Map.delete("entry_hash")
end
