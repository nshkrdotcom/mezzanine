defmodule Mezzanine.Bridges.CitadelBridge.Transport.RuntimeDeps do
  @moduledoc """
  Explicit runtime dependency holder for Mezzanine's Citadel transport.
  """

  alias Mezzanine.Bridges.CitadelBridge.Transport

  defstruct transport: Transport.Direct, transport_opts: []

  @type t :: %__MODULE__{transport: module(), transport_opts: keyword()}

  @spec new(keyword()) :: {:ok, t()} | {:error, map()}
  def new(opts \\ []) when is_list(opts) do
    transport = Keyword.get(opts, :transport, Transport.Direct)
    transport_opts = Keyword.get(opts, :transport_opts, [])

    with :ok <- validate_transport(transport),
         :ok <- validate_transport_opts(transport_opts) do
      {:ok, %__MODULE__{transport: transport, transport_opts: transport_opts}}
    end
  end

  @spec authorize(t(), map(), keyword()) :: Transport.result()
  def authorize(%__MODULE__{} = deps, request, opts \\ [])
      when is_map(request) and is_list(opts) do
    deps.transport.authorize(request, Keyword.merge(deps.transport_opts, opts))
  end

  defp validate_transport(transport) when is_atom(transport) do
    case Code.ensure_loaded(transport) do
      {:module, ^transport} ->
        if function_exported?(transport, :authorize, 2) do
          :ok
        else
          {:error, %{"code" => "invalid_transport", "transport" => inspect(transport)}}
        end

      _other ->
        {:error, %{"code" => "invalid_transport", "transport" => inspect(transport)}}
    end
  end

  defp validate_transport(_transport), do: {:error, %{"code" => "invalid_transport"}}

  defp validate_transport_opts(opts) when is_list(opts), do: :ok
  defp validate_transport_opts(_opts), do: {:error, %{"code" => "invalid_transport_opts"}}
end
