defmodule Mezzanine.IntegrationBridge do
  @moduledoc """
  Narrow direct dispatch bridge into the public Jido Integration platform.
  """

  alias Mezzanine.IntegrationBridge.{
    AuthorizedInvocation,
    DirectRunDispatcher,
    EffectDispatcher,
    EventTranslator,
    LinearSourceDispatcher,
    ReadDispatcher
  }

  alias Mezzanine.Intent.ReadIntent

  @spec invoke_run_intent(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate invoke_run_intent(invocation, opts \\ []), to: DirectRunDispatcher

  @spec dispatch_effect(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate dispatch_effect(invocation, opts \\ []), to: EffectDispatcher

  @spec dispatch_read(ReadIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate dispatch_read(intent, opts \\ []), to: ReadDispatcher

  @spec fetch_linear_candidates(AuthorizedInvocation.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate fetch_linear_candidates(invocation, source_binding, opts \\ []),
    to: LinearSourceDispatcher,
    as: :fetch_candidates

  @spec refresh_linear_issue(AuthorizedInvocation.t(), String.t() | map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate refresh_linear_issue(invocation, issue_or_attrs, source_binding, opts \\ []),
    to: LinearSourceDispatcher,
    as: :refresh_issue

  @spec publish_linear_source(AuthorizedInvocation.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate publish_linear_source(invocation, attrs, opts \\ []),
    to: LinearSourceDispatcher,
    as: :publish_source

  @spec update_linear_issue_state(AuthorizedInvocation.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate update_linear_issue_state(invocation, attrs, opts \\ []),
    to: LinearSourceDispatcher,
    as: :update_issue_state

  @spec to_audit_attrs(map(), map()) :: map()
  defdelegate to_audit_attrs(event, attrs \\ %{}), to: EventTranslator
end
