defmodule Mezzanine.IntegrationBridge do
  @moduledoc """
  Narrow direct dispatch bridge into the public Jido Integration platform.
  """

  alias Mezzanine.IntegrationBridge.{
    AuthorizedInvocation,
    DirectRunDispatcher,
    EffectDispatcher,
    EventTranslator,
    ReadDispatcher
  }

  alias Mezzanine.Intent.ReadIntent

  @spec invoke_run_intent(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate invoke_run_intent(invocation, opts \\ []), to: DirectRunDispatcher

  @spec dispatch_effect(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate dispatch_effect(invocation, opts \\ []), to: EffectDispatcher

  @spec dispatch_read(ReadIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate dispatch_read(intent, opts \\ []), to: ReadDispatcher

  @spec to_audit_attrs(map(), map()) :: map()
  defdelegate to_audit_attrs(event, attrs \\ %{}), to: EventTranslator
end
