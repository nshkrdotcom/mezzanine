defmodule Mezzanine.IntegrationBridge.EffectDispatcher do
  @moduledoc """
  Direct connector-backed effect dispatch for effect intents that already
  resolve to an Integration capability.
  """

  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.IntegrationBridge.DirectRunDispatcher

  @invoke_fun &Jido.Integration.V2.invoke/3

  @spec dispatch_effect(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch_effect(%AuthorizedInvocation{} = invocation, opts \\ []) when is_list(opts) do
    DirectRunDispatcher.invoke_run_intent(
      invocation,
      Keyword.put_new(opts, :invoke_fun, @invoke_fun)
    )
  end
end
