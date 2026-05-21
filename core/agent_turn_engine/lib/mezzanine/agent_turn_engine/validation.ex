defmodule Mezzanine.AgentTurnEngine.Validation do
  @moduledoc false

  @forbidden_keys [
    "access_token",
    "api_key",
    "auth_token",
    "credential",
    "credential_material",
    "credential_ref",
    "endpoint",
    "lower_selector",
    "protocol_module",
    "provider_body",
    "provider_payload",
    "raw_endpoint",
    "raw_payload",
    "raw_prompt",
    "secret",
    "secret_key",
    "secret_ref",
    "token",
    "tool_call",
    "transport_endpoint"
  ]

  @forbidden_value_fragments [
    "A2A.",
    "A2ABridge",
    "AXGrpc",
    "AgentInterop",
    "AxGrpc",
    "AxRuntime",
    "AxSidecar",
    "ControllerService.Exec",
    "Jido.Integration",
    "System.cmd(\"ax\"",
    "ax serve",
    "generated A2A",
    "generated AX proto"
  ]

  @raw_endpoint_prefixes ["http://", "https://", "grpc://", "ws://", "wss://"]

  @spec reject_lower_leakage(map()) :: :ok | {:error, term()}
  def reject_lower_leakage(attrs), do: scan_value(attrs, :root)

  @spec fetch(map(), atom()) :: term()
  def fetch(attrs, field) when is_atom(field) do
    Map.get(attrs, field, Map.get(attrs, Atom.to_string(field)))
  end

  @spec take(map(), struct()) :: map()
  def take(attrs, struct) do
    struct
    |> Map.from_struct()
    |> Map.keys()
    |> Map.new(fn key -> {key, fetch(attrs, key)} end)
  end

  @spec new!(module(), map(), (map() -> {:ok, struct()} | {:error, term()})) :: struct()
  def new!(module, attrs, constructor) do
    case constructor.(attrs) do
      {:ok, struct} ->
        struct

      {:error, reason} ->
        raise ArgumentError, "invalid #{inspect(module)}: #{inspect(reason)}"
    end
  end

  @spec required_binary(map(), atom()) :: :ok | {:error, term()}
  def required_binary(attrs, field) do
    case fetch(attrs, field) do
      value when is_binary(value) and value != "" -> :ok
      nil -> {:error, {:invalid, field, :required}}
      "" -> {:error, {:invalid, field, :required}}
      _other -> {:error, {:invalid, field, :binary}}
    end
  end

  @spec ref(map(), atom(), String.t()) :: :ok | {:error, term()}
  def ref(attrs, field, prefix) do
    with :ok <- required_binary(attrs, field) do
      value = fetch(attrs, field)

      if String.starts_with?(value, prefix) do
        :ok
      else
        {:error, {:invalid, field, {:expected_prefix, prefix}}}
      end
    end
  end

  @spec optional_ref(map(), atom(), String.t()) :: :ok | {:error, term()}
  def optional_ref(attrs, field, prefix) do
    case fetch(attrs, field) do
      nil -> :ok
      _value -> ref(attrs, field, prefix)
    end
  end

  @spec ref_list(map(), atom(), [String.t()]) :: :ok | {:error, term()}
  def ref_list(attrs, field, prefixes) do
    case fetch(attrs, field) do
      refs when is_list(refs) ->
        if Enum.all?(refs, &ref_with_any_prefix?(&1, prefixes)) do
          :ok
        else
          {:error, {:invalid, field, {:expected_prefix, prefixes}}}
        end

      nil ->
        {:error, {:invalid, field, :required}}

      _other ->
        {:error, {:invalid, field, :list}}
    end
  end

  @spec one_of(map(), atom(), [atom()]) :: :ok | {:error, term()}
  def one_of(attrs, field, allowed) do
    case fetch(attrs, field) do
      nil ->
        {:error, {:invalid, field, :required}}

      value ->
        if value in allowed do
          :ok
        else
          {:error, {:invalid, field, {:one_of, allowed}}}
        end
    end
  end

  @spec positive_integer(map(), atom()) :: :ok | {:error, term()}
  def positive_integer(attrs, field) do
    case fetch(attrs, field) do
      value when is_integer(value) and value > 0 -> :ok
      nil -> {:error, {:invalid, field, :required}}
      _other -> {:error, {:invalid, field, :positive_integer}}
    end
  end

  @spec non_negative_integer(map(), atom()) :: :ok | {:error, term()}
  def non_negative_integer(attrs, field) do
    case fetch(attrs, field) do
      value when is_integer(value) and value >= 0 -> :ok
      nil -> {:error, {:invalid, field, :required}}
      _other -> {:error, {:invalid, field, :non_negative_integer}}
    end
  end

  @spec datetime(map(), atom()) :: :ok | {:error, term()}
  def datetime(attrs, field) do
    case fetch(attrs, field) do
      %DateTime{} -> :ok
      nil -> {:error, {:invalid, field, :required}}
      _other -> {:error, {:invalid, field, :datetime}}
    end
  end

  @spec optional_datetime(map(), atom()) :: :ok | {:error, term()}
  def optional_datetime(attrs, field) do
    case fetch(attrs, field) do
      nil -> :ok
      %DateTime{} -> :ok
      _other -> {:error, {:invalid, field, :datetime}}
    end
  end

  @spec sha256(map(), atom()) :: :ok | {:error, term()}
  def sha256(attrs, field) do
    case fetch(attrs, field) do
      "sha256:" <> hash ->
        if byte_size(hash) == 64 and hex?(hash) do
          :ok
        else
          {:error, {:invalid, field, :sha256}}
        end

      nil ->
        {:error, {:invalid, field, :required}}

      _other ->
        {:error, {:invalid, field, :sha256}}
    end
  end

  defp scan_value(%_struct{}, _field), do: :ok

  defp scan_value(value, _field) when is_map(value) do
    value
    |> Enum.reduce_while(:ok, fn {key, child}, :ok ->
      scan_key_value(key, child)
    end)
  end

  defp scan_value(values, field) when is_list(values) do
    values
    |> Enum.reduce_while(:ok, fn value, :ok ->
      case scan_value(value, field) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp scan_value(value, field) when is_binary(value) do
    cond do
      Enum.any?(@raw_endpoint_prefixes, &String.starts_with?(value, &1)) ->
        {:error, {:invalid, field, :raw_endpoint}}

      Enum.any?(@forbidden_value_fragments, &String.contains?(value, &1)) ->
        {:error, {:invalid, field, :forbidden_value}}

      true ->
        :ok
    end
  end

  defp scan_value(_value, _field), do: :ok

  defp scan_key_value(key, value) do
    field = normalize_key(key)

    if field in @forbidden_keys do
      {:halt, {:error, {:invalid, key_atom_or_string(key), :forbidden_key}}}
    else
      case scan_value(value, key_atom_or_string(key)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end
  end

  defp ref_with_any_prefix?(value, prefixes) when is_binary(value) do
    Enum.any?(prefixes, &String.starts_with?(value, &1))
  end

  defp ref_with_any_prefix?(_other, _prefixes), do: false

  defp key_atom_or_string(key) when is_atom(key), do: key
  defp key_atom_or_string(key), do: key

  defp normalize_key(key) when is_atom(key), do: key |> Atom.to_string() |> String.downcase()
  defp normalize_key(key) when is_binary(key), do: String.downcase(key)
  defp normalize_key(key), do: inspect(key)

  defp hex?(hash) do
    hash
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end
end
