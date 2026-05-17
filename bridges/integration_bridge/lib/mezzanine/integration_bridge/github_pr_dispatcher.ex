defmodule Mezzanine.IntegrationBridge.GitHubPrDispatcher do
  @moduledoc """
  Governed GitHub PR/review/status dispatcher.

  GitHub provider effects stay below Citadel and Jido. This module receives an
  already-authorized invocation, supplies provider operation input, and forces
  dispatch through the shared governed lower envelope.
  """

  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.IntegrationBridge.DirectRunDispatcher

  @connector_ref "jido/connectors/github"

  @spec create_pr(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def create_pr(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.pr.create", attrs, opts)

  @spec fetch_pr(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def fetch_pr(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.pr.fetch", attrs, opts)

  @spec list_prs(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def list_prs(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.pr.list", attrs, opts)

  @spec update_pr(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def update_pr(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.pr.update", attrs, opts)

  @spec create_comment(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def create_comment(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.comment.create", attrs, opts)

  @spec list_reviews(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def list_reviews(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.pr.reviews.list", attrs, opts)

  @spec list_review_comments(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def list_review_comments(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.pr.review_comments.list", attrs, opts)

  @spec create_review(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def create_review(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.pr.review.create", attrs, opts)

  @spec create_review_comment(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def create_review_comment(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.pr.review_comment.create", attrs, opts)

  @spec fetch_combined_status(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def fetch_combined_status(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.commit.statuses.get_combined", attrs, opts)

  @spec list_check_runs(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def list_check_runs(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.check_runs.list_for_ref", attrs, opts)

  @spec cleanup_branch(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def cleanup_branch(invocation, attrs, opts \\ []),
    do: dispatch(invocation, "github.git.ref.delete", attrs, opts)

  @spec feedback_sweep(AuthorizedInvocation.t(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def feedback_sweep(%AuthorizedInvocation{} = invocation, attrs, opts \\ [])
      when (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    attrs = normalize_attrs(attrs)

    with {:ok, reviews} <- list_reviews(invocation, attrs, opts),
         {:ok, comments} <- list_review_comments(invocation, attrs, opts),
         {:ok, status} <- fetch_combined_status(invocation, status_attrs(attrs), opts),
         {:ok, checks} <- list_check_runs(invocation, status_attrs(attrs), opts) do
      {:ok,
       %{
         github_feedback_sweep: %{
           repo: value(attrs, :repo),
           pull_number: value(attrs, :pull_number),
           ref: value(attrs, :ref),
           dispatches: [reviews, comments, status, checks],
           operation_receipts:
             Enum.map(
               [reviews, comments, status, checks],
               &Map.get(&1, :github_operation_receipt)
             ),
           review_count: reviews |> output() |> value(:reviews) |> List.wrap() |> length(),
           review_comment_count:
             comments |> output() |> value(:comments) |> List.wrap() |> length(),
           combined_state: status |> output() |> value(:state),
           check_run_count: checks |> output() |> value(:check_runs) |> List.wrap() |> length()
         }
       }}
    end
  end

  defp dispatch(%AuthorizedInvocation{} = invocation, capability_id, attrs, opts)
       when (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    input = normalize_attrs(attrs)

    dispatch_opts =
      opts
      |> Keyword.put(:capability_id, capability_id)
      |> Keyword.put(:input, input)
      |> Keyword.put_new(:lower_runtime_kind, :direct_connector)
      |> Keyword.put_new(:connector_ref, @connector_ref)

    with {:ok, dispatch} <- DirectRunDispatcher.invoke_run_intent(invocation, dispatch_opts) do
      {:ok, Map.put(dispatch, :github_operation_receipt, operation_receipt(dispatch))}
    end
  end

  defp operation_receipt(dispatch) do
    envelope = Map.fetch!(dispatch, :governed_lower_envelope)
    lower_receipt = Map.fetch!(dispatch, :governed_lower_receipt)

    dispatch
    |> Map.get(:operation_receipt, %{})
    |> Map.merge(%{
      capability_id: envelope.capability_id,
      lower_runtime_kind: Atom.to_string(envelope.lower_runtime_kind),
      lower_request_ref: envelope.lower_request_ref,
      lower_receipt_ref: lower_receipt.lower_receipt_ref,
      authority_ref: envelope.authority_ref,
      authority_decision_hash: envelope.authority_decision_hash,
      connector_manifest_ref: envelope.connector_manifest_ref,
      connector_manifest_hash: envelope.connector_manifest_hash,
      capability_negotiation_ref: envelope.capability_negotiation_ref,
      provider_response_ref:
        dispatch |> Map.get(:artifact_refs, []) |> List.wrap() |> List.first(),
      trace_id: envelope.trace_id
    })
    |> compact()
  end

  defp status_attrs(attrs) do
    %{
      repo: value(attrs, :repo),
      ref: value(attrs, :ref) || value(attrs, :head_sha)
    }
    |> maybe_put(:per_page, value(attrs, :per_page))
    |> maybe_put(:page, value(attrs, :page))
    |> compact()
  end

  defp output(dispatch), do: value(dispatch, :output) || %{}

  defp value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp value(_map, _key), do: nil

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp compact(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)
end
