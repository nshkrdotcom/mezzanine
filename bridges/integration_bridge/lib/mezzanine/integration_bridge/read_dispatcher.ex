defmodule Mezzanine.IntegrationBridge.ReadDispatcher do
  @moduledoc """
  Direct connector-backed read dispatch for read intents that already resolve
  to an Integration capability.
  """

  alias MezzanineOpsModel.Intent.ReadIntent

  @invoke_fun &Jido.Integration.V2.invoke/3

  @spec dispatch_read(ReadIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch_read(%ReadIntent{} = intent, opts \\ []) when is_list(opts) do
    invoke_fun = Keyword.get(opts, :invoke_fun, @invoke_fun)
    invoke_opts = Keyword.get(opts, :invoke_opts, [])

    with {:ok, capability_id} <- capability_id(intent),
         {:ok, input} <- input(intent) do
      invoke_fun.(capability_id, input, invoke_opts)
    end
  end

  defp capability_id(intent) do
    case Map.get(intent.metadata, :capability_id) || Map.get(intent.metadata, "capability_id") ||
           Map.get(intent.query, :capability_id) || Map.get(intent.query, "capability_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :unsupported_read_intent}
    end
  end

  defp input(intent) do
    case intent.query do
      %{} = query -> {:ok, query}
      _ -> {:error, :unsupported_read_intent}
    end
  end
end
