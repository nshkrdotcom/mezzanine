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

  @spec publish_source(AuthorizedInvocation.t(), source_role_ref(), map(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def publish_source(
        %AuthorizedInvocation{} = invocation,
        publication_role_ref,
        attrs,
        source_binding,
        opts \\ []
      )
      when (is_atom(publication_role_ref) or is_binary(publication_role_ref)) and
             (is_map(attrs) or is_list(attrs)) and is_map(source_binding) and is_list(opts) do
    attrs_map = Map.new(attrs)

    with {:ok, adapter} <- source_adapter(source_binding, opts) do
      opts = Keyword.put(opts, :publication_role_ref, publication_role_ref)

      if issue_state_publication?(attrs_map) do
        adapter.update_issue_state(invocation, attrs_map, opts)
      else
        adapter.publish_source(invocation, attrs_map, opts)
      end
    end
  end

  @spec publication_allowed_operations(source_role_ref(), map(), map(), keyword()) :: [String.t()]
  def publication_allowed_operations(publication_role_ref, source_binding, attrs, opts \\ [])
      when (is_atom(publication_role_ref) or is_binary(publication_role_ref)) and
             is_map(source_binding) and (is_map(attrs) or is_list(attrs)) and is_list(opts) do
    attrs_map = Map.new(attrs)

    case source_adapter(source_binding, opts) do
      {:ok, adapter} ->
        adapter.publication_allowed_operations(
          publication_role_ref,
          source_binding,
          attrs_map,
          opts
        )

      {:error, _reason} ->
        []
    end
  end

  defp source_adapter(_source_binding, opts) do
    case Keyword.get(opts, :source_adapter) do
      nil -> {:ok, Mezzanine.IntegrationBridge.ProviderAdapters.Linear.SourceDispatcher}
      adapter when is_atom(adapter) -> {:ok, adapter}
      adapter -> {:error, {:invalid_source_adapter, adapter}}
    end
  end

  defp issue_state_publication?(attrs) do
    present?(value(attrs, :state_id)) or present?(value(attrs, :state_name)) or
      value(attrs, :capability_id) == "linear.issues.update" or
      value(attrs, :publication_kind) in [:issue_state_update, "issue_state_update"]
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)

  defp value(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
end
