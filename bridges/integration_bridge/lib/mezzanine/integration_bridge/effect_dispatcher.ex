defmodule Mezzanine.IntegrationBridge.EffectDispatcher do
  @moduledoc """
  Direct connector-backed effect dispatch for effect intents that already
  resolve to an Integration capability.
  """

  alias Mezzanine.Intent.EffectIntent

  @invoke_fun &Jido.Integration.V2.invoke/3

  @spec dispatch_effect(EffectIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch_effect(%EffectIntent{} = intent, opts \\ []) when is_list(opts) do
    invoke_fun = Keyword.get(opts, :invoke_fun, @invoke_fun)
    invoke_opts = Keyword.get(opts, :invoke_opts, [])

    with {:ok, capability_id} <- capability_id(intent),
         {:ok, input} <- input(intent) do
      invoke_fun.(capability_id, input, invoke_opts)
    end
  end

  defp capability_id(intent) do
    case Map.get(intent.metadata, :capability_id) || Map.get(intent.metadata, "capability_id") ||
           Map.get(intent.payload, :capability_id) || Map.get(intent.payload, "capability_id") do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, :unsupported_effect_intent}
    end
  end

  defp input(intent) do
    case Map.get(intent.payload, :input) || Map.get(intent.payload, "input") || intent.payload do
      %{} = payload -> {:ok, payload}
      _ -> {:error, :unsupported_effect_intent}
    end
  end
end
