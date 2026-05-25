defmodule Mezzanine.Bridges.CitadelBridge.Transport.Fixture do
  @moduledoc """
  Deterministic Mezzanine-to-Citadel authority transport for tests.
  """

  @behaviour Mezzanine.Bridges.CitadelBridge.Transport

  @impl true
  def authorize(request, opts) when is_map(request) and is_list(opts) do
    opts
    |> configured_response()
    |> case do
      nil ->
        {:ok,
         %{
           "status" => "authorized",
           "authority_ref" => Map.get(request, "authority_ref", "authority://fixture/citadel"),
           "correlation_ref" =>
             Map.get(request, "correlation_ref", "correlation://fixture/citadel")
         }}

      fun when is_function(fun, 2) ->
        fun.(request, opts)

      fun when is_function(fun, 1) ->
        fun.(request)

      result ->
        result
    end
    |> normalize_result()
  end

  defp configured_response(opts) do
    responses = Keyword.get(opts, :responses, %{})

    Keyword.get(opts, :authorize) || Map.get(responses, :authorize) ||
      Map.get(responses, "authorize")
  end

  defp normalize_result({:ok, result}) when is_map(result), do: {:ok, result}
  defp normalize_result({:error, reason}) when is_map(reason), do: {:error, reason}
  defp normalize_result(result) when is_map(result), do: {:ok, result}

  defp normalize_result(reason),
    do: {:error, error(:invalid_fixture_response, %{"reason" => inspect(reason)})}

  defp error(code, attrs),
    do: Map.merge(%{"code" => Atom.to_string(code), "transport" => "fixture"}, attrs)
end
