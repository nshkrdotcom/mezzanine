defmodule Mezzanine.Bridges.JidoIntegrationBridge.Transport.Distributed do
  @moduledoc """
  Local distributed Jido Integration transport for StackLab proof topology.
  """

  @behaviour Mezzanine.Bridges.JidoIntegrationBridge.Transport

  @default_timeout 5_000

  @impl true
  def submit_invocation(request, opts) when is_map(request) and is_list(opts) do
    remote_call(opts, :submit_invocation, [request, opts])
  end

  @impl true
  def read_invocation(ref, opts) when is_binary(ref) and is_list(opts) do
    remote_call(opts, :read_invocation, [ref, opts])
  end

  defp remote_call(opts, callback, args) do
    with {:ok, node} <- fetch_node(opts),
         {:ok, module} <- fetch_module(opts),
         {:ok, function} <- fetch_function(opts, callback) do
      timeout = Keyword.get(opts, :timeout, @default_timeout)

      try do
        node
        |> :erpc.call(module, function, args, timeout)
        |> normalize_result()
      rescue
        exception -> {:error, error(:unreachable, %{"reason" => Exception.message(exception)})}
      catch
        :exit, reason -> {:error, error(classify_exit(reason), %{"reason" => inspect(reason)})}
      end
    end
  end

  defp fetch_node(opts) do
    case Keyword.get(opts, :node) do
      node when is_atom(node) -> {:ok, node}
      _other -> {:error, error(:missing_node)}
    end
  end

  defp fetch_module(opts) do
    case Keyword.get(opts, :facade_module, Keyword.get(opts, :module)) do
      module when is_atom(module) -> {:ok, module}
      _other -> {:error, error(:missing_facade_module)}
    end
  end

  defp fetch_function(opts, callback) do
    function = Keyword.get(opts, function_option(callback), callback)

    if is_atom(function), do: {:ok, function}, else: {:error, error(:invalid_remote_function)}
  end

  defp function_option(:submit_invocation), do: :submit_invocation_function
  defp function_option(:read_invocation), do: :read_invocation_function

  defp normalize_result({:ok, result}) when is_map(result), do: {:ok, result}
  defp normalize_result({:error, reason}) when is_map(reason), do: {:error, reason}
  defp normalize_result(result) when is_map(result), do: {:ok, result}

  defp normalize_result(reason),
    do: {:error, error(:invalid_remote_response, %{"reason" => inspect(reason)})}

  defp classify_exit(reason) do
    reason
    |> inspect()
    |> String.downcase()
    |> String.contains?("timeout")
    |> case do
      true -> :timeout
      false -> :unreachable
    end
  end

  defp error(code, attrs \\ %{}),
    do: Map.merge(%{"code" => Atom.to_string(code), "transport" => "distributed"}, attrs)
end
