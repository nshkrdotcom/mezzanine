defmodule Mezzanine.IntegrationBridge.SourceDispatcher do
  @moduledoc """
  Generic source operation dispatcher.

  The public bridge receives product role refs plus binding data. Provider
  modules are selected as adapter data and stay below this generic entry point.
  """

  alias Mezzanine.IntegrationBridge.AuthorizedInvocation

  @type source_role_ref :: atom() | String.t()

  @spec fetch_candidates(AuthorizedInvocation.t(), source_role_ref(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def fetch_candidates(
        %AuthorizedInvocation{} = invocation,
        source_role_ref,
        source_binding,
        opts \\ []
      )
      when (is_atom(source_role_ref) or is_binary(source_role_ref)) and is_map(source_binding) and
             is_list(opts) do
    with {:ok, adapter} <- source_adapter(source_binding, opts) do
      adapter.fetch_candidates(
        invocation,
        source_binding,
        Keyword.put(opts, :source_role_ref, source_role_ref)
      )
    end
  end

  @spec current_states(
          AuthorizedInvocation.t(),
          source_role_ref(),
          [String.t()],
          map(),
          keyword()
        ) ::
          {:ok, map()} | {:error, term()}
  def current_states(
        %AuthorizedInvocation{} = invocation,
        source_role_ref,
        issue_ids,
        source_binding,
        opts \\ []
      )
      when (is_atom(source_role_ref) or is_binary(source_role_ref)) and is_list(issue_ids) and
             is_map(source_binding) and is_list(opts) do
    with {:ok, adapter} <- source_adapter(source_binding, opts) do
      adapter.current_issue_states(
        invocation,
        issue_ids,
        source_binding,
        Keyword.put(opts, :source_role_ref, source_role_ref)
      )
    end
  end

  @spec refresh_item(
          AuthorizedInvocation.t(),
          source_role_ref(),
          String.t() | map(),
          map(),
          keyword()
        ) ::
          {:ok, map()} | {:error, term()}
  def refresh_item(
        %AuthorizedInvocation{} = invocation,
        source_role_ref,
        issue_or_attrs,
        source_binding,
        opts \\ []
      )
      when (is_atom(source_role_ref) or is_binary(source_role_ref)) and is_map(source_binding) and
             is_list(opts) do
    with {:ok, adapter} <- source_adapter(source_binding, opts) do
      adapter.refresh_issue(
        invocation,
        issue_or_attrs,
        source_binding,
        Keyword.put(opts, :source_role_ref, source_role_ref)
      )
    end
  end

  @spec normalize_page(source_role_ref(), map(), map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def normalize_page(source_role_ref, output, envelope, source_binding, opts \\ [])
      when (is_atom(source_role_ref) or is_binary(source_role_ref)) and is_map(output) and
             is_map(envelope) and is_map(source_binding) and is_list(opts) do
    with {:ok, adapter} <- source_adapter(source_binding, opts) do
      adapter.normalize_candidate_page(
        output,
        envelope,
        source_binding,
        Keyword.put(opts, :source_role_ref, source_role_ref)
      )
    end
  end

  @spec read_allowed_operations(source_role_ref(), map(), keyword()) :: [String.t()]
  def read_allowed_operations(source_role_ref, source_binding, opts \\ [])
      when (is_atom(source_role_ref) or is_binary(source_role_ref)) and is_map(source_binding) and
             is_list(opts) do
    case source_adapter(source_binding, opts) do
      {:ok, adapter} -> adapter.read_allowed_operations(source_role_ref, source_binding, opts)
      {:error, _reason} -> []
    end
  end

  @spec publish_source(AuthorizedInvocation.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def publish_source(%AuthorizedInvocation{} = invocation, attrs, opts \\ [])
      when (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    attrs_map = attrs |> Map.new()

    with {:ok, adapter} <- source_adapter(attrs_map, opts) do
      adapter.publish_source(invocation, attrs, opts)
    end
  end

  @spec update_source_state(AuthorizedInvocation.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def update_source_state(%AuthorizedInvocation{} = invocation, attrs, opts \\ [])
      when (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    attrs_map = attrs |> Map.new()

    with {:ok, adapter} <- source_adapter(attrs_map, opts) do
      adapter.update_issue_state(invocation, attrs, opts)
    end
  end

  defp source_adapter(_source_binding, opts) do
    case Keyword.get(opts, :source_adapter) do
      nil -> {:ok, Mezzanine.IntegrationBridge.ProviderAdapters.Linear.SourceDispatcher}
      adapter when is_atom(adapter) -> {:ok, adapter}
      adapter -> {:error, {:invalid_source_adapter, adapter}}
    end
  end
end
