defmodule Mezzanine.IntegrationBridge do
  @moduledoc """
  Narrow direct dispatch bridge into the public Jido Integration platform.
  """

  alias Mezzanine.IntegrationBridge.{
    AuthorizedInvocation,
    DirectRunDispatcher,
    EffectDispatcher,
    EventTranslator,
    LinearCredentialIngress,
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

  @spec collect_evidence(term(), map(), map() | keyword() | nil, keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate collect_evidence(evidence_role_ref, attrs, evidence_binding, opts \\ []),
    to: Mezzanine.IntegrationBridge.EvidenceDispatcher

  @spec evidence_allowed_operations(term(), map() | keyword() | nil, map(), keyword()) ::
          [String.t()]
  defdelegate evidence_allowed_operations(evidence_role_ref, evidence_binding, attrs, opts \\ []),
    to: Mezzanine.IntegrationBridge.EvidenceDispatcher

  @spec invoke_resource_effect(term(), map(), map() | keyword() | nil, keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate invoke_resource_effect(
                resource_effect_role_ref,
                attrs,
                resource_effect_binding,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.ResourceEffectDispatcher

  @spec resource_effect_allowed_operations(term(), map() | keyword() | nil, map(), keyword()) ::
          [String.t()]
  defdelegate resource_effect_allowed_operations(
                resource_effect_role_ref,
                resource_effect_binding,
                attrs,
                opts \\ []
              ),
              to: Mezzanine.IntegrationBridge.ResourceEffectDispatcher

  @spec to_audit_attrs(map(), map()) :: map()
  defdelegate to_audit_attrs(event, attrs \\ %{}), to: EventTranslator
end
