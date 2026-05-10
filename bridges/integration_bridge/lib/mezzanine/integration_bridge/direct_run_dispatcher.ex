defmodule Mezzanine.IntegrationBridge.DirectRunDispatcher do
  @moduledoc """
  Direct public-platform dispatch for narrow run-intent cases.
  """

  alias Jido.Integration.V2.GovernedLowerEnvelope
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation

  @invoke_fun &Jido.Integration.V2.invoke/3

  @spec invoke_run_intent(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def invoke_run_intent(%AuthorizedInvocation{} = invocation, opts \\ []) when is_list(opts) do
    invoke_fun = Keyword.get(opts, :invoke_fun, @invoke_fun)
    invoke_opts = Keyword.get(opts, :invoke_opts, [])

    capability_id =
      Keyword.get(opts, :capability_id, AuthorizedInvocation.default_capability!(invocation))

    with {:ok, envelope} <-
           AuthorizedInvocation.governed_lower_envelope(invocation, capability_id, opts),
         :ok <- require_dispatchable(envelope, invoke_opts) do
      input =
        invocation
        |> AuthorizedInvocation.invoke_input(capability_id)
        |> merge_dispatch_input(Keyword.get(opts, :input, %{}))
        |> Map.put(:governed_lower_envelope, GovernedLowerEnvelope.to_map(envelope))

      invoke_opts = Keyword.put(invoke_opts, :governed_lower_envelope, envelope)

      invoke_fun.(capability_id, input, invoke_opts)
      |> attach_governed_receipt(envelope)
    end
  end

  defp require_dispatchable(envelope, invoke_opts) do
    if GovernedLowerEnvelope.dispatchable?(envelope) or
         tre_adapter_enabled?(envelope, invoke_opts) do
      :ok
    else
      {:error,
       AuthorizedInvocation.governed_lower_denial(
         envelope,
         :lower_runtime_unavailable,
         "lower runtime kind #{inspect(envelope.lower_runtime_kind)} is reserved or unavailable"
       )}
    end
  end

  defp tre_adapter_enabled?(%GovernedLowerEnvelope{lower_runtime_kind: :tre_rhai}, invoke_opts)
       when is_list(invoke_opts) do
    case Keyword.get(invoke_opts, :tre_adapter) do
      module when is_atom(module) ->
        Code.ensure_loaded?(module) and function_exported?(module, :execute, 3)

      _other ->
        false
    end
  end

  defp tre_adapter_enabled?(%GovernedLowerEnvelope{}, _invoke_opts), do: false

  defp attach_governed_receipt({:ok, result}, envelope) when is_map(result) do
    receipt = AuthorizedInvocation.governed_lower_receipt!(envelope, :succeeded, result)

    {:ok,
     result
     |> Map.put(:governed_lower_envelope, envelope)
     |> Map.put(:governed_lower_receipt, receipt)}
  end

  defp attach_governed_receipt({:error, result}, envelope) when is_map(result) do
    receipt = AuthorizedInvocation.governed_lower_receipt!(envelope, :failed, result)

    {:error,
     result
     |> Map.put(:governed_lower_envelope, envelope)
     |> Map.put(:governed_lower_receipt, receipt)}
  end

  defp attach_governed_receipt(other, _envelope), do: other

  defp merge_dispatch_input(input, extra_input) when is_map(extra_input) do
    Map.merge(input, extra_input)
  end

  defp merge_dispatch_input(input, extra_input) when is_list(extra_input) do
    Map.merge(input, Map.new(extra_input))
  end

  defp merge_dispatch_input(_input, extra_input) do
    raise ArgumentError,
          "dispatch input must be a map or keyword list, got: #{inspect(extra_input)}"
  end
end
