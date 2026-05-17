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
    ReadDispatcher
  }

  alias Mezzanine.Intent.ReadIntent

  @spec invoke_run_intent(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate invoke_run_intent(invocation, opts \\ []), to: DirectRunDispatcher

  @spec dispatch_effect(AuthorizedInvocation.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate dispatch_effect(invocation, opts \\ []), to: EffectDispatcher

  @spec dispatch_read(ReadIntent.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate dispatch_read(intent, opts \\ []), to: ReadDispatcher

  @spec fetch_source_candidates(AuthorizedInvocation.t(), atom() | String.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate fetch_source_candidates(invocation, source_role_ref, source_binding, opts \\ []),
    to: Mezzanine.IntegrationBridge.SourceDispatcher,
    as: :fetch_candidates

  @spec refresh_source_item(
          AuthorizedInvocation.t(),
          atom() | String.t(),
          String.t() | map(),
          map(),
          keyword()
        ) ::
          {:ok, map()} | {:error, term()}
  defdelegate refresh_source_item(
                invocation,
                source_role_ref,
                issue_or_attrs,
                source_binding,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.SourceDispatcher,
              as: :refresh_item

  @spec fetch_source_current_states(
          AuthorizedInvocation.t(),
          atom() | String.t(),
          [String.t()],
          map(),
          keyword()
        ) ::
          {:ok, map()} | {:error, term()}
  defdelegate fetch_source_current_states(
                invocation,
                source_role_ref,
                issue_ids,
                source_binding,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.SourceDispatcher,
              as: :current_states

  @spec normalize_source_page(atom() | String.t(), map(), map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate normalize_source_page(
                source_role_ref,
                output,
                envelope,
                source_binding,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.SourceDispatcher,
              as: :normalize_page

  @spec source_read_allowed_operations(atom() | String.t(), map(), keyword()) :: [String.t()]
  defdelegate source_read_allowed_operations(source_role_ref, source_binding, opts \\ []),
    to: Mezzanine.IntegrationBridge.SourceDispatcher,
    as: :read_allowed_operations

  @spec publish_source(AuthorizedInvocation.t(), atom() | String.t(), map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate publish_source(invocation, publication_role_ref, attrs, source_binding, opts \\ []),
    to: Mezzanine.IntegrationBridge.SourceDispatcher

  @spec source_publication_allowed_operations(atom() | String.t(), map(), map(), keyword()) ::
          [String.t()]
  defdelegate source_publication_allowed_operations(
                publication_role_ref,
                source_binding,
                attrs,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.SourceDispatcher,
              as: :publication_allowed_operations

  @spec prepare_linear_api_key_invocation(String.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate prepare_linear_api_key_invocation(api_key, attrs, opts \\ []),
    to: LinearCredentialIngress,
    as: :prepare_api_key_invocation

  @spec prepare_linear_connection_invocation(String.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate prepare_linear_connection_invocation(connection_id, attrs, opts \\ []),
    to: LinearCredentialIngress,
    as: :prepare_connection_invocation

  @spec execute_dynamic_tool(AuthorizedInvocation.t(), String.t(), term(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate execute_dynamic_tool(invocation, tool_name, arguments, opts \\ []),
    to: LinearGraphQLToolExecutor

  @spec invoke_runtime_operation(
          term(),
          term(),
          term(),
          map(),
          map() | keyword() | nil,
          keyword()
        ) :: {:ok, map()} | {:error, term()}
  defdelegate invoke_runtime_operation(
                invocation,
                runtime_role_ref,
                operation_role_ref,
                attrs,
                runtime_binding,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.RuntimeDispatcher

  @spec runtime_operation_allowed_operations(
          term(),
          term(),
          map() | keyword() | nil,
          map(),
          keyword()
        ) ::
          [String.t()]
  defdelegate runtime_operation_allowed_operations(
                runtime_role_ref,
                operation_role_ref,
                runtime_binding,
                attrs,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.RuntimeDispatcher

  @spec invoke_runtime_tool(
          AuthorizedInvocation.t(),
          term(),
          term(),
          term(),
          map() | keyword() | nil,
          keyword()
        ) :: {:ok, map()} | {:error, term()}
  defdelegate invoke_runtime_tool(
                invocation,
                tool_role_ref,
                operation_role_ref,
                arguments,
                tool_binding,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.ToolDispatcher

  @spec runtime_tool_allowed_operations(
          term(),
          term(),
          map() | keyword() | nil,
          term(),
          keyword()
        ) ::
          [String.t()]
  defdelegate runtime_tool_allowed_operations(
                tool_role_ref,
                operation_role_ref,
                tool_binding,
                attrs,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.ToolDispatcher

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

  @spec create_github_comment(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate create_github_comment(invocation, attrs, opts \\ []),
    to: GitHubPrDispatcher,
    as: :create_comment

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
