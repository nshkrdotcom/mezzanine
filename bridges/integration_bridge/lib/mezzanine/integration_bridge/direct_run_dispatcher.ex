defmodule Mezzanine.IntegrationBridge.DirectRunDispatcher do
  @moduledoc """
  Direct public-platform dispatch for narrow run-intent cases.
  """

  alias Mezzanine.IntegrationBridge.AuthorizedInvocation

  @invoke_fun &Jido.Integration.V2.invoke/3

  @spec invoke_run_intent(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def invoke_run_intent(%AuthorizedInvocation{} = invocation, opts \\ []) when is_list(opts) do
    invoke_fun = Keyword.get(opts, :invoke_fun, @invoke_fun)
    invoke_opts = Keyword.get(opts, :invoke_opts, [])

    capability_id =
      Keyword.get(opts, :capability_id, AuthorizedInvocation.default_capability!(invocation))

    :ok = AuthorizedInvocation.authorize_capability!(invocation, capability_id)

    invoke_fun.(
      capability_id,
      AuthorizedInvocation.invoke_input(invocation, capability_id),
      invoke_opts
    )
  end
end
