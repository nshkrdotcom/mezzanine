defmodule Mezzanine.CitadelBridge do
  @moduledoc """
  Public lowering helpers from Mezzanine run intents into Citadel host ingress.
  """

  alias Citadel.HostIngress
  alias Citadel.HostIngress.RequestContext
  alias Mezzanine.CitadelBridge.{AuthorityAssembler, EventMapper, RunIntentCompiler}
  alias MezzanineOpsModel.Intent.RunIntent

  @spec compile_run_request(RunIntent.t(), map()) ::
          {:ok, Citadel.HostIngress.RunRequest.t()} | {:error, term()}
  def compile_run_request(%RunIntent{} = intent, attrs \\ %{}) when is_map(attrs) do
    RunIntentCompiler.compile(intent, attrs)
  end

  @spec build_request_context(RunIntent.t(), map()) ::
          {:ok, RequestContext.t()} | {:error, term()}
  def build_request_context(%RunIntent{} = intent, attrs \\ %{}) when is_map(attrs) do
    AuthorityAssembler.request_context(intent, attrs)
  end

  @spec compile_submission(RunIntent.t(), map(), [map()], keyword()) ::
          {:ok, HostIngress.InvocationCompiler.compiled()}
          | {:rejected, Citadel.DecisionRejection.t()}
          | {:error, term()}
  def compile_submission(%RunIntent{} = intent, attrs \\ %{}, policy_packs \\ [], opts \\ [])
      when is_map(attrs) and is_list(policy_packs) and is_list(opts) do
    with {:ok, run_request} <- compile_run_request(intent, attrs),
         {:ok, request_context} <- build_request_context(intent, attrs) do
      HostIngress.compile_run_request(run_request, request_context, policy_packs, opts)
    end
  end

  @spec submit_run_intent(HostIngress.t(), RunIntent.t(), map(), keyword()) ::
          HostIngress.submission_result() | {:error, term()}
  def submit_run_intent(%HostIngress{} = ingress, %RunIntent{} = intent, attrs \\ %{}, opts \\ [])
      when is_map(attrs) and is_list(opts) do
    with {:ok, run_request} <- compile_run_request(intent, attrs),
         {:ok, request_context} <- build_request_context(intent, attrs) do
      HostIngress.submit_run_request(ingress, run_request, request_context, opts)
    end
  end

  @spec to_audit_attrs(map(), map()) :: map()
  defdelegate to_audit_attrs(event, attrs \\ %{}), to: EventMapper
end
