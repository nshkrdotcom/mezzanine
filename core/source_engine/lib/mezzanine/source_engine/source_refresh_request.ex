defmodule Mezzanine.SourceEngine.SourceRefreshRequest do
  @moduledoc """
  Owner-local source refresh request contract.

  This module marks a refresh as requested for the source coordinator without
  polling provider APIs or reconciling source state. Callers must authorize
  tenant and installation scope before invoking it.
  """

  alias Mezzanine.SourceEngine.SourceCursor

  @required_keys [
    :tenant_id,
    :installation_id,
    :subject_id,
    :source_binding_id,
    :trace_id,
    :causation_id,
    :actor_ref,
    :idempotency_key
  ]

  @spec request(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def request(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with :ok <- require_fields(attrs),
         :ok <- ensure_actor_tenant(attrs) do
      cursor = %SourceCursor{
        source_binding_id: required!(attrs, :source_binding_id),
        cursor: value(attrs, :cursor),
        refresh_requested?: true
      }

      {:ok,
       %{
         cursor: cursor,
         refresh_requested?: true,
         source_binding_id: cursor.source_binding_id,
         subject_id: required!(attrs, :subject_id),
         installation_id: required!(attrs, :installation_id),
         tenant_id: required!(attrs, :tenant_id),
         trace_id: required!(attrs, :trace_id),
         causation_id: required!(attrs, :causation_id),
         actor_ref: normalize_map(required!(attrs, :actor_ref)),
         idempotency_key: required!(attrs, :idempotency_key),
         safe_action: "request_source_refresh",
         lower_effect_started?: false,
         reconcile_started?: false
       }}
    end
  end

  defp require_fields(attrs) do
    missing =
      Enum.reject(@required_keys, fn key ->
        present?(value(attrs, key))
      end)

    case missing do
      [] -> :ok
      fields -> {:error, {:missing_source_refresh_fields, fields}}
    end
  end

  defp ensure_actor_tenant(attrs) do
    tenant_id = required!(attrs, :tenant_id)

    attrs
    |> required!(:actor_ref)
    |> normalize_map()
    |> Map.get("tenant_id")
    |> case do
      nil -> :ok
      ^tenant_id -> :ok
      _other -> {:error, :operator_actor_tenant_mismatch}
    end
  end

  defp present?(value), do: not is_nil(value) and value != ""

  defp required!(attrs, key) do
    case value(attrs, key) do
      nil -> raise ArgumentError, "missing required source refresh field #{inspect(key)}"
      "" -> raise ArgumentError, "missing required source refresh field #{inspect(key)}"
      value -> value
    end
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)

  defp value(attrs, key) when is_map(attrs) do
    case Map.fetch(attrs, key) do
      {:ok, value} -> value
      :error -> Map.get(attrs, Atom.to_string(key))
    end
  end

  defp normalize_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
    |> Map.new()
  end

  defp normalize_map(_value), do: %{}

  defp normalize_value(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value
end
