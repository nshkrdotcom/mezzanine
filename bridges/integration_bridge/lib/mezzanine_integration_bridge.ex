defmodule Mezzanine.IntegrationBridge do
  @moduledoc """
  Narrow direct dispatch bridge into the public Jido Integration platform.
  """

  alias Mezzanine.IntegrationBridge.{
    DirectRunDispatcher,
    EffectDispatcher,
    EventTranslator,
    ReadDispatcher
  }

  alias Mezzanine.Intent.{EffectIntent, ReadIntent, RunIntent}

  @spec invoke_run_intent(RunIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate invoke_run_intent(intent, opts \\ []), to: DirectRunDispatcher

  @spec dispatch_effect(EffectIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate dispatch_effect(intent, opts \\ []), to: EffectDispatcher

  @spec dispatch_read(ReadIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate dispatch_read(intent, opts \\ []), to: ReadDispatcher

  @spec to_audit_attrs(map(), map()) :: map()
  defdelegate to_audit_attrs(event, attrs \\ %{}), to: EventTranslator
end
