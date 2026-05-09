defmodule Mezzanine.IntegrationBridge.LinearSourceDispatcher do
  @moduledoc """
  Governed Linear source read and publication dispatcher.

  This module stays below AppKit and above Jido. It receives an already
  authorized Citadel invocation, supplies the Linear operation input, dispatches
  through the governed lower envelope, and normalizes provider-safe source
  outputs with `Mezzanine.SourceEngine`.
  """

  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.IntegrationBridge.DirectRunDispatcher
  alias Mezzanine.SourceEngine.LinearSourceFlow

  @spec fetch_candidates(AuthorizedInvocation.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def fetch_candidates(%AuthorizedInvocation{} = invocation, source_binding, opts \\ [])
      when is_map(source_binding) and is_list(opts) do
    with {:ok, input, normalize_opts, viewer_dispatch} <-
           candidate_fetch_input(invocation, source_binding, opts),
         {:ok, dispatch} <-
           dispatch_linear(invocation, "linear.issues.list", input, opts),
         {:ok, normalized} <-
           LinearSourceFlow.normalize_candidate_page(
             output!(dispatch),
             source_envelope(invocation, normalize_opts),
             source_binding
           ) do
      {:ok,
       dispatch
       |> Map.put(:source_intake, normalized)
       |> maybe_put(:viewer_resolution, viewer_dispatch)}
    end
  end

  @spec refresh_issue(AuthorizedInvocation.t(), String.t() | map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def refresh_issue(
        %AuthorizedInvocation{} = invocation,
        issue_or_attrs,
        source_binding,
        opts \\ []
      )
      when is_map(source_binding) and is_list(opts) do
    with {:ok, input} <- LinearSourceFlow.refresh_issue_input(issue_or_attrs, opts),
         {:ok, dispatch} <-
           dispatch_linear(invocation, "linear.issues.retrieve", input, opts),
         {:ok, normalized} <-
           LinearSourceFlow.normalize_issue_refresh(
             output!(dispatch),
             source_envelope(invocation, opts),
             source_binding
           ) do
      {:ok, Map.put(dispatch, :source_refresh, normalized)}
    end
  end

  @spec update_issue_state(AuthorizedInvocation.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def update_issue_state(%AuthorizedInvocation{} = invocation, attrs, opts \\ [])
      when (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    with {:ok, input} <- LinearSourceFlow.issue_state_update_input(attrs) do
      dispatch_linear(invocation, "linear.issues.update", input, opts)
    end
  end

  @spec publish_source(AuthorizedInvocation.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def publish_source(%AuthorizedInvocation{} = invocation, attrs, opts \\ [])
      when (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    attrs = normalize_attrs(attrs)

    with {:ok, {capability_id, input}} <- LinearSourceFlow.publication_input(attrs),
         {:ok, dispatch} <- dispatch_linear(invocation, capability_id, input, opts),
         {:ok, receipt} <- LinearSourceFlow.publication_receipt(dispatch, attrs) do
      {:ok, dispatch |> Map.put(:source_publication_receipt, receipt)}
    else
      {:error, reason} ->
        maybe_create_fallback(invocation, attrs, reason, opts)
    end
  end

  defp maybe_create_fallback(invocation, attrs, update_error, opts) do
    if create_fallback_after_update?(attrs, update_error) do
      fallback_attrs =
        attrs
        |> Map.delete(:comment_id)
        |> Map.delete("comment_id")
        |> Map.put(:allow_create_fallback?, true)

      with {:ok, {"linear.comments.create", input}} <-
             LinearSourceFlow.publication_input(fallback_attrs),
           {:ok, dispatch} <- dispatch_linear(invocation, "linear.comments.create", input, opts),
           {:ok, receipt} <-
             LinearSourceFlow.publication_receipt(
               dispatch,
               Map.put(fallback_attrs, :capability_id, "linear.comments.create")
             ) do
        {:ok,
         dispatch
         |> Map.put(
           :source_publication_receipt,
           Map.put(receipt, :fallback_from, "linear.comments.update")
         )}
      end
    else
      {:error, update_error}
    end
  end

  defp dispatch_linear(invocation, capability_id, input, opts) do
    dispatch_opts =
      opts
      |> Keyword.put(:capability_id, capability_id)
      |> Keyword.put(:input, input)
      |> Keyword.put_new(:lower_runtime_kind, :direct_connector)
      |> Keyword.put_new(:connector_ref, "jido/connectors/linear")

    DirectRunDispatcher.invoke_run_intent(invocation, dispatch_opts)
  end

  defp candidate_fetch_input(invocation, source_binding, opts) do
    case LinearSourceFlow.candidate_fetch_input(source_binding, opts) do
      {:ok, input} ->
        {:ok, input, opts, nil}

      {:error, :linear_viewer_required_for_me_assignee} ->
        with {:ok, viewer_dispatch} <-
               dispatch_linear(invocation, "linear.users.get_self", %{}, opts),
             {:ok, viewer} <- viewer_from_dispatch(viewer_dispatch),
             viewer_opts <- Keyword.put(opts, :viewer, viewer),
             {:ok, input} <- LinearSourceFlow.candidate_fetch_input(source_binding, viewer_opts) do
          {:ok, input, viewer_opts, viewer_dispatch}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp viewer_from_dispatch(dispatch) do
    case output!(dispatch) do
      %{user: %{} = user} -> {:ok, user}
      %{"user" => %{} = user} -> {:ok, user}
      _missing -> {:error, :missing_linear_viewer}
    end
  end

  defp source_envelope(%AuthorizedInvocation{} = invocation, opts) do
    envelope =
      opts
      |> Keyword.get(:source_envelope, %{})
      |> normalize_attrs()
      |> Map.put_new(:tenant_id, invocation.tenant_id)
      |> Map.put_new(:installation_id, invocation.installation_id)
      |> Map.put_new(:trace_id, invocation.trace_id)
      |> Map.put_new(:causation_id, invocation.submission_dedupe_key)
      |> Map.put_new(:authorization_scope, %{"tenant_id" => invocation.tenant_id})
      |> Map.put_new(:actor_ref, %{
        "kind" => "source_dispatcher",
        "id" => "mezzanine-integration-bridge",
        "tenant_id" => invocation.tenant_id
      })

    case Keyword.get(opts, :viewer) do
      %{} = viewer -> Map.put_new(envelope, :viewer, viewer)
      _other -> envelope
    end
  end

  defp output!(dispatch) do
    Map.get(dispatch, :output) || Map.get(dispatch, "output") || %{}
  end

  defp create_fallback_after_update?(attrs, update_error) do
    fallback_allowed?(attrs) and retryable_update_error?(update_error)
  end

  defp fallback_allowed?(attrs) do
    Map.get(attrs, :allow_create_fallback?) == true or
      Map.get(attrs, "allow_create_fallback?") == true or
      Map.get(attrs, :create_fallback?) == true or
      Map.get(attrs, "create_fallback?") == true
  end

  defp retryable_update_error?(%{reason: reason}), do: retryable_update_error?(reason)

  defp retryable_update_error?(%{code: code})
       when code in ["linear.not_found", :linear_not_found], do: true

  defp retryable_update_error?(reason) do
    reason
    |> inspect()
    |> String.contains?("not_found")
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
