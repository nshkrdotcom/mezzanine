defmodule Mezzanine.Bridges.OuterBrainBridge.Transport.Fixture do
  @moduledoc """
  Deterministic Mezzanine-to-OuterBrain context transport for tests.
  """

  @behaviour Mezzanine.Bridges.OuterBrainBridge.Transport

  @impl true
  def compile_context(request, opts) when is_map(request) and is_list(opts) do
    reply(opts, :compile_context, [request, opts], fn ->
      {:ok,
       %{
         "context_packet_ref" =>
           Map.get(request, "context_packet_ref", "context://fixture/packet"),
         "packet_hash" => Map.get(request, "packet_hash", "sha256:fixture-context-packet"),
         "correlation_ref" => Map.get(request, "correlation_ref", "correlation://fixture/context")
       }}
    end)
  end

  @impl true
  def readback_context(ref, opts) when is_binary(ref) and is_list(opts) do
    reply(opts, :readback_context, [ref, opts], fn ->
      {:ok, %{"context_packet_ref" => ref, "packet_hash" => "sha256:fixture-context-packet"}}
    end)
  end

  defp reply(opts, callback, args, default) do
    opts
    |> configured_response(callback)
    |> case do
      nil -> default.()
      fun when is_function(fun, length(args)) -> apply(fun, args)
      fun when is_function(fun, length(args) - 1) -> apply(fun, Enum.drop(args, -1))
      result -> result
    end
    |> normalize_result()
  end

  defp configured_response(opts, callback) do
    responses = Keyword.get(opts, :responses, %{})

    Keyword.get(opts, callback) || Map.get(responses, callback) ||
      Map.get(responses, Atom.to_string(callback))
  end

  defp normalize_result({:ok, result}) when is_map(result), do: {:ok, result}
  defp normalize_result({:error, reason}) when is_map(reason), do: {:error, reason}
  defp normalize_result(result) when is_map(result), do: {:ok, result}

  defp normalize_result(reason),
    do: {:error, error(:invalid_fixture_response, %{"reason" => inspect(reason)})}

  defp error(code, attrs),
    do: Map.merge(%{"code" => Atom.to_string(code), "transport" => "fixture"}, attrs)
end
