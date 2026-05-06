defmodule Mezzanine.CoordinationEngine.Validation do
  @moduledoc false

  @forbidden_raw_fields [
    :api_key,
    :authorization_header,
    :credential_body,
    :memory_body,
    :message_body,
    :model_output,
    :native_auth_file_contents,
    :oauth_secret,
    :operator_private_payload,
    :prompt,
    :provider_payload,
    :raw_auth_material,
    :raw_credentials,
    :raw_message,
    :raw_model_output,
    :raw_prompt,
    :raw_provider_payload,
    :raw_tool_input,
    :raw_tool_output,
    :secret,
    :tool_body,
    :tool_output,
    :workflow_history,
    "api_key",
    "authorization_header",
    "credential_body",
    "memory_body",
    "message_body",
    "model_output",
    "native_auth_file_contents",
    "oauth_secret",
    "operator_private_payload",
    "prompt",
    "provider_payload",
    "raw_auth_material",
    "raw_credentials",
    "raw_message",
    "raw_model_output",
    "raw_prompt",
    "raw_provider_payload",
    "raw_tool_input",
    "raw_tool_output",
    "secret",
    "tool_body",
    "tool_output",
    "workflow_history"
  ]

  @spec reject_raw(map() | list() | term()) :: :ok | {:error, term()}
  def reject_raw(value), do: reject_raw(value, [])

  @spec require_binary(map(), atom()) :: {:ok, String.t()} | {:error, term()}
  def require_binary(attrs, key) when is_map(attrs) do
    case fetch(attrs, key) do
      value when is_binary(value) ->
        if String.trim(value) == "" do
          {:error, {:missing_required_ref, key}}
        else
          {:ok, value}
        end

      _other ->
        {:error, {:missing_required_ref, key}}
    end
  end

  @spec require_string_list(map(), atom()) :: {:ok, [String.t()]} | {:error, term()}
  def require_string_list(attrs, key) when is_map(attrs) do
    case fetch(attrs, key) do
      values when is_list(values) and values != [] ->
        if Enum.all?(values, &valid_binary?/1) do
          {:ok, values}
        else
          {:error, {:missing_required_ref, key}}
        end

      value when is_binary(value) ->
        if String.trim(value) == "" do
          {:error, {:missing_required_ref, key}}
        else
          {:ok, [value]}
        end

      _other ->
        {:error, {:missing_required_ref, key}}
    end
  end

  @spec optional_string_list(map(), atom()) :: [String.t()]
  def optional_string_list(attrs, key) when is_map(attrs) do
    case fetch(attrs, key) do
      values when is_list(values) -> Enum.filter(values, &valid_binary?/1)
      value when is_binary(value) and value != "" -> [value]
      _other -> []
    end
  end

  @spec fetch(map(), atom()) :: term()
  def fetch(attrs, key) when is_map(attrs),
    do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp reject_raw(%_struct{} = value, path), do: value |> Map.from_struct() |> reject_raw(path)

  defp reject_raw(%{} = map, path) do
    Enum.reduce_while(map, :ok, fn {key, value}, :ok ->
      reject_raw_entry(key, value, path)
    end)
  end

  defp reject_raw(values, path) when is_list(values) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      case reject_raw(value, path) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp reject_raw(_value, _path), do: :ok

  defp reject_raw_entry(key, _value, _path) when key in @forbidden_raw_fields,
    do: {:halt, {:error, {:forbidden_raw_field, key}}}

  defp reject_raw_entry(key, value, path) do
    case reject_raw(value, [key | path]) do
      :ok -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp valid_binary?(value), do: is_binary(value) and String.trim(value) != ""
end
