defmodule Mezzanine.Substrate.BindingResolverTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Substrate.BindingResolver
  alias Mezzanine.Substrate.BindingResolver.Error
  alias Mezzanine.Substrate.OperationContext
  alias Mezzanine.Substrate.OperationRequest
  alias Mezzanine.Substrate.PayloadEnvelope

  test "resolves a governed operation plan from binding, manifest, and authority facts" do
    context = context!()
    request = request!(context, :source_read)

    assert {:ok, plan} =
             BindingResolver.resolve_plan(context, request, resolver_attrs())

    assert plan.binding_ref == "binding://tenant-a/source-primary"
    assert plan.manifest_ref == "manifest://ticket/v1"
    assert plan.operation_ref == "operation://ticket/search"
    assert plan.operation_class == :source_read
    assert plan.adapter_ref == "adapter://ticket/search"
    assert plan.credential_scope_ref == "credential-scope://tenant-a/ticket"
    assert plan.side_effect_class == :read
    assert plan.input_schema_ref == "schema://ticket/search/input"
    assert plan.output_schema_ref == "schema://ticket/search/output"
    assert plan.authority_packet_ref == "authority://tenant-a/request-a"
    assert plan.metadata.binding_epoch == 7
    assert plan.metadata.binding_kind == :source
    assert plan.metadata.connector_ref == "connector://ticket"
    assert plan.metadata.credential_lease_ref == "credential-lease://tenant-a/ticket"
    assert plan.metadata.required_scopes == ["tickets.read"]
  end

  test "builds a lower governed invocation envelope without product role refs" do
    context = context!()
    request = request!(context, :source_read)
    payload = payload!()

    {:ok, plan} = BindingResolver.resolve_plan(context, request, resolver_attrs())

    assert {:ok, envelope} =
             BindingResolver.build_envelope(context, plan, payload,
               invocation_ref: "invocation://tenant-a/request-a/source-read"
             )

    refute :operation_role_ref in Map.keys(envelope)
    refute :source_role_ref in Map.keys(envelope)
    assert envelope.operation_plan.operation_plan_ref == plan.operation_plan_ref
    assert envelope.tenant_ref == context.tenant_ref
    assert envelope.installation_ref == context.installation_ref
    assert envelope.trace_ref == context.trace_ref
    assert envelope.idempotency_key == context.idempotency_key
  end

  test "fails closed when binding epoch is stale" do
    context = context!()
    request = request!(context, :source_read)

    assert {:error, %Error{} = error} =
             BindingResolver.resolve_plan(
               context,
               request,
               resolver_attrs(expected_binding_epoch: 6)
             )

    assert error.reason == {:stale_binding_epoch, 6, 7}
    refute error.retryable
    assert error.recovery_owner == :platform_operator
    assert error.propagation_target == :binding_registry
    assert error.operator_action == :restart_with_current_binding_epoch
    assert error.details == %{expected_binding_epoch: 6, actual_binding_epoch: 7}
  end

  test "fails closed on manifest digest drift" do
    context = context!()
    request = request!(context, :source_read)

    attrs =
      resolver_attrs(
        operation_descriptor:
          Map.put(operation_descriptor(), :manifest_digest, "sha256:descriptor-drift")
      )

    assert {:error, %Error{} = error} =
             BindingResolver.resolve_plan(context, request, attrs)

    assert error.reason ==
             {:manifest_digest_mismatch, "sha256:descriptor-drift", "sha256:ticket-v1"}

    refute error.retryable
    assert error.propagation_target == :manifest_registry
  end

  test "fails closed on operation class mismatch" do
    context = context!()
    request = request!(context, :runtime_session)

    assert {:error, %Error{} = error} =
             BindingResolver.resolve_plan(context, request, resolver_attrs())

    assert error.reason == {:field_mismatch, :operation_class, :runtime_session, :source_read}
    assert error.blast_radius == :single_invocation
    assert error.operator_action == :inspect_binding_resolution
  end

  test "fails closed when side effect class expands beyond the binding dependency" do
    context = context!()
    request = request!(context, :source_read)

    attrs =
      resolver_attrs(
        operation_descriptor: Map.put(operation_descriptor(), :side_effect_class, :write)
      )

    assert {:error, %Error{} = error} =
             BindingResolver.resolve_plan(context, request, attrs)

    assert error.reason == {:side_effect_class_expanded, :write, :read}
    assert error.recovery_owner == :authority_boundary
    assert error.operator_action == :review_side_effect_policy
  end

  test "fails closed when required scopes expand beyond binding dependency" do
    context = context!()
    request = request!(context, :source_read)

    attrs =
      resolver_attrs(
        operation_descriptor:
          Map.put(operation_descriptor(), :required_scopes, ["tickets.read", "tickets.write"])
      )

    assert {:error, %Error{} = error} =
             BindingResolver.resolve_plan(context, request, attrs)

    assert error.reason == {:required_scope_expansion, ["tickets.write"]}
    assert error.recovery_owner == :authority_boundary
    assert error.propagation_target == :authority_policy
    assert error.operator_action == :review_authority_scope_policy
  end

  test "fails closed when a credential lease is missing" do
    context = context!()
    request = request!(context, :source_read)

    attrs =
      resolver_attrs()
      |> Map.delete(:credential_lease_ref)

    assert {:error, %Error{} = error} =
             BindingResolver.resolve_plan(context, request, attrs)

    assert error.reason == :missing_credential_lease
    assert error.retryable
    assert error.recovery_owner == :credential_authority
    assert error.operator_action == :rematerialize_credential_lease
  end

  test "fails closed when side-effecting operations have no confirmation policy" do
    context = context!()
    request = request!(context, :source_write)

    attrs =
      resolver_attrs(
        operation_role: "write",
        binding_resolution: write_binding_resolution(),
        operation_descriptor: write_operation_descriptor()
      )

    assert {:error, %Error{} = error} =
             BindingResolver.resolve_plan(context, request, attrs)

    assert error.reason == {:missing_confirmation_policy, :write}
    assert error.recovery_owner == :product_pack_owner
    assert error.propagation_target == :confirmation_policy
    assert error.operator_action == :add_confirmation_policy
  end

  defp resolver_attrs(overrides \\ []) do
    %{
      operation_role: "read",
      operation_plan_ref: "operation-plan://tenant-a/request-a/source-read",
      expected_binding_epoch: 7,
      authority_decision_ref: "authority-decision://tenant-a/request-a",
      authority_packet_ref: "authority://tenant-a/request-a",
      credential_lease_ref: "credential-lease://tenant-a/ticket",
      binding_resolution: binding_resolution(),
      operation_descriptor: operation_descriptor()
    }
    |> Map.merge(Map.new(overrides))
  end

  defp context! do
    {:ok, context} =
      OperationContext.new(%{
        operation_context_ref: "operation-context://tenant-a/request-a",
        actor_ref: "actor://tenant-a/user-a",
        tenant_ref: "tenant://tenant-a",
        installation_ref: "installation://tenant-a/product-a/install-a",
        trace_ref: "trace://tenant-a/trace-a",
        request_ref: "request://tenant-a/request-a",
        idempotency_key: "idempotency://tenant-a/request-a",
        authority_packet_ref: "authority://tenant-a/request-a"
      })

    context
  end

  defp request!(context, operation_class) do
    {:ok, request} =
      OperationRequest.new(%{
        operation_request_ref: "operation-request://tenant-a/request-a",
        operation_context_ref: context.operation_context_ref,
        operation_role_ref: "operation-role://case-source/read",
        operation_class: operation_class,
        authority_packet_ref: "authority://tenant-a/request-a",
        payload: payload!()
      })

    request
  end

  defp payload! do
    {:ok, payload} =
      PayloadEnvelope.new(%{
        payload_ref: "payload://tenant-a/request-a",
        storage_mode: :inline,
        schema_ref: "schema://ticket/search/input",
        redaction_ref: "redaction://standard",
        data: %{query: "needs review"}
      })

    payload
  end

  defp binding_resolution do
    %{
      binding_epoch: 7,
      descriptor: %{
        binding_ref: "binding://tenant-a/source-primary",
        binding_kind: :source,
        connector_ref: "connector://ticket",
        manifest_ref: "manifest://ticket/v1",
        operation_refs: %{"read" => "operation://ticket/search"},
        credential_binding_ref: "credential-scope://tenant-a/ticket",
        runtime_family: nil,
        binding_epoch: 7
      },
      manifest_dependencies: [
        %{
          binding_ref: "binding://tenant-a/source-primary",
          operation_role: "read",
          operation_ref: "operation://ticket/search",
          operation_class: :source_read,
          side_effect_class: :read,
          credential_scope_ref: "credential-scope://tenant-a/ticket",
          manifest_digest: "sha256:ticket-v1",
          required_scopes: ["tickets.read"]
        }
      ]
    }
  end

  defp operation_descriptor do
    %{
      manifest_ref: "manifest://ticket/v1",
      operation_ref: "operation://ticket/search",
      operation_class: :source_read,
      adapter_ref: "adapter://ticket/search",
      credential_scope_ref: "credential-scope://tenant-a/ticket",
      side_effect_class: :read,
      input_schema_ref: "schema://ticket/search/input",
      output_schema_ref: "schema://ticket/search/output",
      manifest_digest: "sha256:ticket-v1",
      required_scopes: ["tickets.read"]
    }
  end

  defp write_binding_resolution do
    %{
      binding_epoch: 7,
      descriptor: %{
        binding_ref: "binding://tenant-a/source-publication",
        binding_kind: :source_publication,
        connector_ref: "connector://ticket",
        manifest_ref: "manifest://ticket/v1",
        operation_refs: %{"write" => "operation://ticket/comment"},
        credential_binding_ref: "credential-scope://tenant-a/ticket",
        binding_epoch: 7
      },
      manifest_dependencies: [
        %{
          binding_ref: "binding://tenant-a/source-publication",
          operation_role: "write",
          operation_ref: "operation://ticket/comment",
          operation_class: :source_write,
          side_effect_class: :write,
          credential_scope_ref: "credential-scope://tenant-a/ticket",
          manifest_digest: "sha256:ticket-v1",
          required_scopes: ["tickets.write"]
        }
      ]
    }
  end

  defp write_operation_descriptor do
    %{
      manifest_ref: "manifest://ticket/v1",
      operation_ref: "operation://ticket/comment",
      operation_class: :source_write,
      adapter_ref: "adapter://ticket/comment",
      credential_scope_ref: "credential-scope://tenant-a/ticket",
      side_effect_class: :write,
      input_schema_ref: "schema://ticket/comment/input",
      output_schema_ref: "schema://ticket/comment/output",
      manifest_digest: "sha256:ticket-v1",
      required_scopes: ["tickets.write"]
    }
  end
end
