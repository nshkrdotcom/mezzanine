defmodule Mezzanine.Core.GovernedEffects.AuthorityPacket do
  @moduledoc """
  Mezzanine-internal authority decision view for governed effects.

  Citadel remains the authority owner. This struct is the Mezzanine-consumable
  lifecycle view of that decision.
  """

  alias Mezzanine.Core.GovernedEffects.Support

  @decisions [:allow, :deny, :review, :downgrade, :revoke]
  @required [:authority_ref, :decision, :tenant_ref, :actor_ref]
  @optional [
    :command_ref,
    :trace_ref,
    :policy_refs,
    :risk_class,
    :budget_refs,
    :residency_refs,
    :reason,
    :expiry
  ]
  @fields @required ++ @optional
  @defaults %{policy_refs: [], budget_refs: [], residency_refs: []}

  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  def decisions, do: @decisions

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- Support.reject_unknown(attrs, @fields),
         :ok <- Support.require_fields(attrs, @required),
         {:ok, decision} <-
           Support.bounded_atom(Support.required(attrs, :decision), @decisions, :invalid_decision) do
      values =
        attrs
        |> Support.values(@fields, @defaults)
        |> Map.put(:decision, decision)

      packet = struct!(__MODULE__, values)

      with :ok <- packet |> to_map() |> Support.ensure_serializable() do
        {:ok, packet}
      end
    end
  end

  def new(_attrs), do: {:error, :invalid_authority_packet_attrs}

  def new!(attrs), do: bang(new(attrs))

  def to_map(%__MODULE__{} = packet), do: Support.boundary_map(packet, @fields)
  def encode!(%__MODULE__{} = packet), do: packet |> to_map() |> Support.encode!()
  def digest(%__MODULE__{} = packet), do: packet |> to_map() |> Support.digest()

  defp bang({:ok, value}), do: value

  defp bang({:error, reason}),
    do: raise(ArgumentError, "invalid authority packet: #{inspect(reason)}")
end
