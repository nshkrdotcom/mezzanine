defmodule Mezzanine.Bridges.JidoIntegrationBridge.Transport.Direct do
  @moduledoc """
  In-process Jido Integration transport for monolith mode.
  """

  @behaviour Mezzanine.Bridges.JidoIntegrationBridge.Transport

  @impl true
  def submit_invocation(request, opts) when is_map(request) and is_list(opts) do
    call(opts, :submit_invocation, [request, opts])
  end

  @impl true
  def read_invocation(ref, opts) when is_binary(ref) and is_list(opts) do
    call(opts, :read_invocation, [ref, opts])
  end

  defp call(opts, callback, args) do
    with {:ok, target} <- fetch_target(opts),
         {:ok, function} <- fetch_function(opts, callback),
         {:ok, apply_args} <- apply_args(target, function, args) do
      target
      |> apply(function, apply_args)
      |> normalize_result()
    end
  end

  defp fetch_target(opts) do
    case Keyword.get(opts, :target, Keyword.get(opts, :module)) do
      target when is_atom(target) -> {:ok, target}
      _other -> {:error, error(:missing_direct_target)}
    end
  end

  defp fetch_function(opts, callback) do
    function = Keyword.get(opts, function_option(callback), callback)

    if is_atom(function), do: {:ok, function}, else: {:error, error(:invalid_direct_function)}
  end

  defp apply_args(target, function, args) do
    args_without_opts = Enum.drop(args, -1)

    cond do
      function_exported?(target, function, length(args)) -> {:ok, args}
      function_exported?(target, function, length(args_without_opts)) -> {:ok, args_without_opts}
      true -> {:error, error(:direct_target_unavailable, %{"target" => inspect(target)})}
    end
  end

  defp function_option(:submit_invocation), do: :submit_invocation_function
  defp function_option(:read_invocation), do: :read_invocation_function

  defp normalize_result({:ok, result}) when is_map(result), do: {:ok, result}
  defp normalize_result({:error, reason}) when is_map(reason), do: {:error, reason}
  defp normalize_result(result) when is_map(result), do: {:ok, result}

  defp normalize_result(reason),
    do: {:error, error(:invalid_direct_response, %{"reason" => inspect(reason)})}

  defp error(code, attrs \\ %{}),
    do: Map.merge(%{"code" => Atom.to_string(code), "transport" => "direct"}, attrs)
end
