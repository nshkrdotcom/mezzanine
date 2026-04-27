defmodule Mezzanine.IntegrationBridge.EffectDispatcher do
  @moduledoc """
  Direct connector-backed effect dispatch for effect intents that already
  resolve to an Integration capability.
  """

  alias Mezzanine.IntegrationBridge.AuthorizedInvocation

  @invoke_fun &Jido.Integration.V2.invoke/3

  @spec dispatch_effect(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch_effect(%AuthorizedInvocation{} = invocation, opts \\ []) when is_list(opts) do
    invoke_fun = Keyword.get(opts, :invoke_fun, @invoke_fun)
    invoke_opts = Keyword.get(opts, :invoke_opts, [])
    capability_id = Keyword.get(opts, :capability_id, AuthorizedInvocation.default_capability!(invocation))

    :ok = AuthorizedInvocation.authorize_capability!(invocation, capability_id)

    invoke_fun.(capability_id, AuthorizedInvocation.invoke_input(invocation, capability_id), invoke_opts)
  end
end
