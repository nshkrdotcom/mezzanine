defmodule Mezzanine.RuntimeScheduler.InstallationLease do
  @moduledoc """
  Lease contract for one installation-scoped runtime owner.
  """

  defstruct [
    :installation_id,
    :holder,
    :lease_id,
    :epoch,
    :compiled_pack_revision,
    :expires_at
  ]

  @type t :: %__MODULE__{
          installation_id: String.t(),
          holder: String.t(),
          lease_id: String.t(),
          epoch: non_neg_integer(),
          compiled_pack_revision: pos_integer(),
          expires_at: DateTime.t()
        }

  @required_fields [
    :installation_id,
    :holder,
    :lease_id,
    :epoch,
    :compiled_pack_revision,
    :expires_at
  ]

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- ensure_required_fields(attrs),
         {:ok, epoch} <- fetch_non_negative_integer(attrs, :epoch),
         {:ok, revision} <- fetch_positive_integer(attrs, :compiled_pack_revision),
         {:ok, expires_at} <- fetch_datetime(attrs, :expires_at) do
      {:ok,
       %__MODULE__{
         installation_id: Map.fetch!(attrs, :installation_id),
         holder: Map.fetch!(attrs, :holder),
         lease_id: Map.fetch!(attrs, :lease_id),
         epoch: epoch,
         compiled_pack_revision: revision,
         expires_at: expires_at
       }}
    end
  end

  @spec expired?(t(), DateTime.t()) :: boolean()
  def expired?(%__MODULE__{expires_at: expires_at}, %DateTime{} = now) do
    DateTime.compare(expires_at, now) != :gt
  end

  defp ensure_required_fields(attrs) do
    missing =
      Enum.reject(@required_fields, fn field ->
        Map.has_key?(attrs, field)
      end)

    case missing do
      [] -> :ok
      _ -> {:error, {:missing_fields, missing}}
    end
  end

  defp fetch_non_negative_integer(attrs, field) do
    case Map.fetch!(attrs, field) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _ -> {:error, {:invalid_non_negative_integer, field}}
    end
  end

  defp fetch_positive_integer(attrs, field) do
    case Map.fetch!(attrs, field) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _ -> {:error, {:invalid_positive_integer, field}}
    end
  end

  defp fetch_datetime(attrs, field) do
    case Map.fetch!(attrs, field) do
      %DateTime{} = value -> {:ok, value}
      _ -> {:error, {:invalid_datetime, field}}
    end
  end
end
