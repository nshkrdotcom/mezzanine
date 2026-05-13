defmodule Mezzanine.IntegrationBridge do
  @moduledoc """
  Narrow direct dispatch bridge into the public Jido Integration platform.
  """

  alias Mezzanine.IntegrationBridge.{
    AuthorizedInvocation,
    DirectRunDispatcher,
    EffectDispatcher,
    EventTranslator,
    GitHubPrDispatcher,
    LinearCredentialIngress,
    LinearGraphQLToolExecutor,
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

  @spec fetch_linear_current_issue_states(
          AuthorizedInvocation.t(),
          [String.t()],
          map(),
          keyword()
        ) ::
          {:ok, map()} | {:error, term()}
  defdelegate fetch_linear_current_issue_states(
                invocation,
                issue_ids,
                source_binding,
                opts \\ []
              ),
              to: LinearSourceDispatcher,
              as: :current_issue_states

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

  @spec prepare_linear_api_key_invocation(String.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate prepare_linear_api_key_invocation(api_key, attrs, opts \\ []),
    to: LinearCredentialIngress,
    as: :prepare_api_key_invocation

  @spec execute_dynamic_tool(AuthorizedInvocation.t(), String.t(), term(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate execute_dynamic_tool(invocation, tool_name, arguments, opts \\ []),
    to: LinearGraphQLToolExecutor

  @spec execute_linear_graphql_tool(AuthorizedInvocation.t(), term(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate execute_linear_graphql_tool(invocation, arguments, opts \\ []),
    to: LinearGraphQLToolExecutor

  @spec create_github_pr(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate create_github_pr(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :create_pr

  @spec fetch_github_pr(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate fetch_github_pr(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :fetch_pr

  @spec list_github_prs(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate list_github_prs(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :list_prs

  @spec update_github_pr(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate update_github_pr(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :update_pr

  @spec list_github_pr_reviews(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate list_github_pr_reviews(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :list_reviews

  @spec list_github_pr_review_comments(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate list_github_pr_review_comments(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :list_review_comments

  @spec create_github_pr_review(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate create_github_pr_review(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :create_review

  @spec create_github_pr_review_comment(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate create_github_pr_review_comment(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :create_review_comment

  @spec fetch_github_combined_status(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate fetch_github_combined_status(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :fetch_combined_status

  @spec list_github_check_runs(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate list_github_check_runs(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :list_check_runs

  @spec sweep_github_pr_feedback(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate sweep_github_pr_feedback(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :feedback_sweep

  @spec cleanup_github_branch(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate cleanup_github_branch(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :cleanup_branch

  @spec to_audit_attrs(map(), map()) :: map()
  defdelegate to_audit_attrs(event, attrs \\ %{}), to: EventTranslator
end
