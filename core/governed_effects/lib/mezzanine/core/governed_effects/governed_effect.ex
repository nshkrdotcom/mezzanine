defmodule Mezzanine.Core.GovernedEffects.GovernedEffect do
  @moduledoc """
  Mezzanine-internal lifecycle state for one governed effect.
  """

  alias Mezzanine.Core.GovernedEffects.Support

  @statuses [
    :proposed,
    :authorized,
    :dispatched,
    :receipt_received,
    :reduced,
    :projected,
    :completed,
    :denied,
    :compensating
  ]

  @required [:effect_ref, :effect_type, :command_ref, :tenant_ref, :status]
  @optional [
    :actor_ref,
    :installation_ref,
    :authority_ref,
    :risk_class,
    :preconditions,
    :dispatch_ref,
    :receipt_ref,
    :compensation_posture,
    :expected_version,
    :trace_ref,
    :created_at,
    :updated_at
  ]
  @fields @required ++ @optional
  @defaults %{preconditions: []}

  @enforce_keys @required
  defstruct @fields

  @type t :: %__MODULE__{}

  def statuses, do: @statuses

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- Support.reject_unknown(attrs, @fields),
         :ok <- Support.require_fields(attrs, @required),
         {:ok, status} <-
           Support.bounded_atom(Support.required(attrs, :status), @statuses, :invalid_status) do
      values =
        attrs
        |> Support.values(@fields, @defaults)
        |> Map.put(:status, status)

      effect = struct!(__MODULE__, values)

      with :ok <- effect |> to_map() |> Support.ensure_serializable() do
        {:ok, effect}
      end
    end
  end

  def new(_attrs), do: {:error, :invalid_governed_effect_attrs}

  def new!(attrs), do: bang(new(attrs))

  def to_map(%__MODULE__{} = effect), do: Support.boundary_map(effect, @fields)
  def encode!(%__MODULE__{} = effect), do: effect |> to_map() |> Support.encode!()
  def digest(%__MODULE__{} = effect), do: effect |> to_map() |> Support.digest()

  defp bang({:ok, value}), do: value

  defp bang({:error, reason}),
    do: raise(ArgumentError, "invalid governed effect: #{inspect(reason)}")
end
