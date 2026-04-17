defmodule Mezzanine.IntegrationBridge.DirectRunDispatcher do
  @moduledoc """
  Direct public-platform dispatch for narrow run-intent cases.
  """

  alias Mezzanine.Intent.RunIntent

  @invoke_fun &Jido.Integration.V2.invoke/3

  @spec invoke_run_intent(RunIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def invoke_run_intent(%RunIntent{} = intent, opts \\ []) when is_list(opts) do
    invoke_fun = Keyword.get(opts, :invoke_fun, @invoke_fun)
    invoke_opts = Keyword.get(opts, :invoke_opts, [])

    invoke_fun.(intent.capability, intent.input, invoke_opts)
  end
end
