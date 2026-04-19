defmodule Mezzanine.Leasing.AuthorizationScope do
  @moduledoc """
  Typed caller scope for read and stream lease authorization.
  """

  @type t :: %__MODULE__{
          tenant_id: String.t(),
          installation_id: String.t() | nil,
          installation_revision: non_neg_integer(),
          activation_epoch: non_neg_integer(),
          lease_epoch: non_neg_integer(),
          subject_id: Ecto.UUID.t() | nil,
          execution_id: Ecto.UUID.t() | nil,
          trace_id: String.t() | nil,
          actor_ref: map() | nil,
          authorized_at: DateTime.t() | nil
        }

  @enforce_keys [:tenant_id]
  defstruct tenant_id: nil,
            installation_id: nil,
            installation_revision: nil,
            activation_epoch: nil,
            lease_epoch: nil,
            subject_id: nil,
            execution_id: nil,
            trace_id: nil,
            actor_ref: nil,
            authorized_at: nil

  @spec new(t() | map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(%__MODULE__{} = scope), do: normalize(scope)

  def new(attrs) when is_map(attrs) or is_list(attrs) do
    {:ok, build!(attrs)}
  rescue
    error in ArgumentError -> {:error, error}
  end

  @spec new!(t() | map() | keyword()) :: t()
  def new!(%__MODULE__{} = scope) do
    case normalize(scope) do
      {:ok, normalized} -> normalized
      {:error, %ArgumentError{} = error} -> raise error
    end
  end

  def new!(attrs), do: build!(attrs)

  @spec dump(t()) :: map()
  def dump(%__MODULE__{} = scope) do
    %{
      tenant_id: scope.tenant_id,
      installation_id: scope.installation_id,
      installation_revision: scope.installation_revision,
      activation_epoch: scope.activation_epoch,
      lease_epoch: scope.lease_epoch,
      subject_id: scope.subject_id,
      execution_id: scope.execution_id,
      trace_id: scope.trace_id,
      actor_ref: scope.actor_ref,
      authorized_at: scope.authorized_at
    }
  end

  defp normalize(%__MODULE__{} = scope) do
    {:ok, build!(dump(scope))}
  rescue
    error in ArgumentError -> {:error, error}
  end

  defp build!(attrs) do
    attrs = Map.new(attrs)

    %__MODULE__{
      tenant_id: required_string!(attrs, :tenant_id),
      installation_id: optional_string!(Map.get(attrs, :installation_id)),
      installation_revision: required_non_neg_integer!(attrs, :installation_revision),
      activation_epoch: required_non_neg_integer!(attrs, :activation_epoch),
      lease_epoch: required_non_neg_integer!(attrs, :lease_epoch),
      subject_id: optional_string!(Map.get(attrs, :subject_id)),
      execution_id: optional_string!(Map.get(attrs, :execution_id)),
      trace_id: optional_string!(Map.get(attrs, :trace_id)),
      actor_ref: optional_actor_ref!(Map.get(attrs, :actor_ref)),
      authorized_at: optional_datetime!(Map.get(attrs, :authorized_at))
    }
  end

  defp required_string!(attrs, key) do
    attrs
    |> Map.get(key)
    |> case do
      value when is_binary(value) and value != "" ->
        value

      value ->
        raise ArgumentError, "#{inspect(key)} must be a non-empty string, got: #{inspect(value)}"
    end
  end

  defp required_non_neg_integer!(attrs, key) do
    attrs
    |> Map.get(key)
    |> case do
      value when is_integer(value) and value >= 0 ->
        value

      value ->
        raise ArgumentError,
              "#{inspect(key)} must be a non-negative integer, got: #{inspect(value)}"
    end
  end

  defp optional_string!(nil), do: nil
  defp optional_string!(value) when is_binary(value) and value != "", do: value

  defp optional_string!(value) do
    raise ArgumentError,
          "optional scope strings must be non-empty strings, got: #{inspect(value)}"
  end

  defp optional_actor_ref!(nil), do: nil
  defp optional_actor_ref!(actor_ref) when is_map(actor_ref), do: actor_ref

  defp optional_actor_ref!(actor_ref) do
    raise ArgumentError, "actor_ref must be a map, got: #{inspect(actor_ref)}"
  end

  defp optional_datetime!(nil), do: nil
  defp optional_datetime!(%DateTime{} = authorized_at), do: authorized_at

  defp optional_datetime!(authorized_at) do
    raise ArgumentError, "authorized_at must be a DateTime, got: #{inspect(authorized_at)}"
  end
end
