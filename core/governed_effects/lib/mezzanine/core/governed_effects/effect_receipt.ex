defmodule Mezzanine.Core.GovernedEffects.EffectReceipt do
  @moduledoc """
  Mezzanine-internal reduced receipt for a governed effect.
  """

  alias Mezzanine.Core.GovernedEffects.Support

  @statuses [:success, :failure, :partial, :timeout, :compensated, :denied, :cancelled]
  @required [:receipt_ref, :effect_ref, :status]
  @optional [
    :lower_receipt_ref,
    :lower_facts,
    :projection_updates,
    :evidence_refs,
    :trace_ref,
    :completed_at
  ]
  @fields @required ++ @optional
  @defaults %{lower_facts: %{}, projection_updates: [], evidence_refs: []}

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

      receipt = struct!(__MODULE__, values)

      with :ok <- receipt |> to_map() |> Support.ensure_serializable() do
        {:ok, receipt}
      end
    end
  end

  def new(_attrs), do: {:error, :invalid_effect_receipt_attrs}

  def new!(attrs), do: bang(new(attrs))

  def to_map(%__MODULE__{} = receipt), do: Support.boundary_map(receipt, @fields)
  def encode!(%__MODULE__{} = receipt), do: receipt |> to_map() |> Support.encode!()
  def digest(%__MODULE__{} = receipt), do: receipt |> to_map() |> Support.digest()

  defp bang({:ok, value}), do: value

  defp bang({:error, reason}),
    do: raise(ArgumentError, "invalid effect receipt: #{inspect(reason)}")
end
