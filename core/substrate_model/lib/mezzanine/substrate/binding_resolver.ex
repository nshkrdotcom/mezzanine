defmodule Mezzanine.Substrate.BindingResolver.Error do
  @moduledoc "Fail-closed binding resolution error with operational recovery metadata."

  @enforce_keys [
    :reason,
    :retryable,
    :recovery_owner,
    :blast_radius,
    :propagation_target,
    :operator_action
  ]
  defstruct @enforce_keys ++ [details: %{}]

  @type t :: %__MODULE__{}
end

defmodule Mezzanine.Substrate.BindingResolver do
  @moduledoc """
  Pure binding-to-invocation reducer for the generic substrate.

  Runtime callers are expected to fetch durable binding snapshots, manifest
  operation descriptors, credential leases, and authority decisions at the
  boundary. This module only validates those facts and composes the hot-path
  dispatch snapshot used by lower invocation code.
  """

  alias Mezzanine.Substrate.{
    GovernedInvocationEnvelope,
    OperationContext,
    OperationPlanValidator,
    OperationRequest,
    ResolvedOperationPlan
  }

  alias __MODULE__.Error

  @known_atoms [
    :collect,
    :evidence,
    :evidence_collection,
    :lower_read,
    :read,
    :resource_effect,
    :review_decision,
    :run,
    :runtime,
    :runtime_session,
    :runtime_tool,
    :runtime_tool_invocation,
    :source,
    :source_publication,
    :source_read,
    :source_write,
    :trace_replay,
    :write
  ]
  @side_effecting_classes [:write, :resource_effect]

  @type descriptor :: map()
  @type attrs :: keyword() | map()
  @type resolve_result :: {:ok, ResolvedOperationPlan.t()} | {:error, Error.t()}
  @type envelope_result :: {:ok, GovernedInvocationEnvelope.t()} | {:error, Error.t()}

  @spec resolve_plan(OperationContext.t(), OperationRequest.t(), attrs()) :: resolve_result()
  def resolve_plan(%OperationContext{} = context, %OperationRequest{} = request, attrs)
      when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         {:ok, role} <- required_attr(attrs, :operation_role),
         {:ok, resolution} <- required_map(attrs, :binding_resolution),
         {:ok, descriptor} <- required_map(attrs, :operation_descriptor),
         {:ok, binding_descriptor} <- binding_descriptor(resolution),
         {:ok, dependency} <- dependency_for_role(resolution, role),
         :ok <- validate_epoch(attrs, binding_descriptor, resolution),
         :ok <- validate_binding_role(binding_descriptor, dependency, role),
         :ok <- validate_descriptor(request, descriptor, binding_descriptor, dependency),
         :ok <- validate_authority_material(attrs, descriptor, binding_descriptor),
         {:ok, plan} <-
           build_plan(context, request, attrs, descriptor, binding_descriptor, dependency),
         :ok <- OperationPlanValidator.validate_complete(plan) do
      {:ok, plan}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, fail_closed(reason)}
    end
  end

  @spec build_envelope(OperationContext.t(), ResolvedOperationPlan.t(), term(), attrs()) ::
          envelope_result()
  def build_envelope(
        %OperationContext{} = context,
        %ResolvedOperationPlan{} = plan,
        payload,
        attrs \\ []
      )
      when is_map(attrs) or is_list(attrs) do
    with {:ok, attrs} <- normalize_attrs(attrs),
         {:ok, invocation_ref} <- required_attr(attrs, :invocation_ref),
         {:ok, envelope} <-
           GovernedInvocationEnvelope.new(%{
             invocation_ref: invocation_ref,
             operation_context_ref: context.operation_context_ref,
             tenant_ref: context.tenant_ref,
             installation_ref: context.installation_ref,
             trace_ref: context.trace_ref,
             idempotency_key: context.idempotency_key,
             operation_plan: plan,
             payload: payload,
             metadata:
               attrs
               |> attr(:metadata)
               |> normalize_metadata()
           }) do
      {:ok, envelope}
    else
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> {:error, fail_closed(reason)}
    end
  end

  defp build_plan(context, request, attrs, descriptor, binding_descriptor, dependency) do
    operation_plan_ref =
      attr(attrs, :operation_plan_ref) ||
        "operation-plan://#{context.request_ref}/#{attr(dependency, :operation_role)}"

    plan_attrs = %{
      operation_plan_ref: operation_plan_ref,
      operation_context_ref: context.operation_context_ref,
      binding_ref: attr(binding_descriptor, :binding_ref),
      manifest_ref: attr(binding_descriptor, :manifest_ref),
      operation_ref: attr(dependency, :operation_ref),
      operation_class: request.operation_class,
      adapter_ref: attr(descriptor, :adapter_ref),
      credential_scope_ref: attr(dependency, :credential_scope_ref),
      side_effect_class: normalize_atom(attr(descriptor, :side_effect_class)),
      input_schema_ref: attr(descriptor, :input_schema_ref),
      output_schema_ref: attr(descriptor, :output_schema_ref),
      lane_policy_ref: attr(descriptor, :lane_policy_ref),
      authority_packet_ref: request.authority_packet_ref || context.authority_packet_ref,
      metadata:
        plan_metadata(attrs, descriptor, binding_descriptor, dependency)
        |> normalize_metadata()
    }

    ResolvedOperationPlan.new(plan_attrs)
  end

  defp plan_metadata(attrs, descriptor, binding_descriptor, dependency) do
    attrs
    |> attr(:metadata)
    |> normalize_metadata()
    |> Map.merge(%{
      binding_epoch: attr(binding_descriptor, :binding_epoch),
      binding_kind: normalize_atom(attr(binding_descriptor, :binding_kind)),
      connector_ref: attr(binding_descriptor, :connector_ref),
      credential_lease_ref: attr(attrs, :credential_lease_ref),
      manifest_digest: attr(descriptor, :manifest_digest),
      operation_role: attr(dependency, :operation_role),
      required_scopes: normalized_list(attr(descriptor, :required_scopes)),
      runtime_family: attr(binding_descriptor, :runtime_family)
    })
  end

  defp binding_descriptor(resolution) do
    case attr(resolution, :descriptor) do
      nil -> {:ok, resolution}
      descriptor when is_map(descriptor) -> {:ok, descriptor}
      descriptor -> {:error, {:invalid_binding_descriptor, descriptor}}
    end
  end

  defp dependency_for_role(resolution, role) do
    dependencies =
      resolution
      |> attr(:manifest_dependencies)
      |> dependency_items()

    case Enum.find(dependencies, &(to_string(attr(&1, :operation_role)) == to_string(role))) do
      nil -> {:error, {:missing_manifest_dependency, role}}
      dependency -> {:ok, dependency}
    end
  end

  defp dependency_items(%{"items" => items}) when is_list(items), do: items
  defp dependency_items(%{items: items}) when is_list(items), do: items
  defp dependency_items(items) when is_list(items), do: items
  defp dependency_items(_items), do: []

  defp validate_epoch(attrs, binding_descriptor, resolution) do
    expected_epoch = attr(attrs, :expected_binding_epoch)

    descriptor_epoch =
      attr(binding_descriptor, :binding_epoch) || attr(resolution, :binding_epoch)

    cond do
      is_nil(expected_epoch) -> :ok
      expected_epoch == descriptor_epoch -> :ok
      true -> {:error, {:stale_binding_epoch, expected_epoch, descriptor_epoch}}
    end
  end

  defp validate_binding_role(binding_descriptor, dependency, role) do
    operation_refs = attr(binding_descriptor, :operation_refs) || %{}
    expected_operation_ref = lookup_role(operation_refs, role)

    cond do
      is_nil(expected_operation_ref) ->
        {:error, {:missing_binding_operation_role, role}}

      expected_operation_ref != attr(dependency, :operation_ref) ->
        {:error,
         {:binding_manifest_operation_mismatch,
          %{
            role: role,
            binding_operation_ref: expected_operation_ref,
            dependency_operation_ref: attr(dependency, :operation_ref)
          }}}

      attr(binding_descriptor, :binding_ref) != attr(dependency, :binding_ref) ->
        {:error, {:binding_manifest_binding_mismatch, role}}

      true ->
        :ok
    end
  end

  defp validate_descriptor(request, descriptor, binding_descriptor, dependency) do
    with :ok <-
           equal_field(
             :manifest_ref,
             attr(descriptor, :manifest_ref),
             attr(binding_descriptor, :manifest_ref)
           ),
         :ok <-
           equal_field(
             :operation_ref,
             attr(descriptor, :operation_ref),
             attr(dependency, :operation_ref)
           ),
         :ok <-
           equal_field(
             :operation_class,
             request.operation_class,
             normalized_operation_class(dependency)
           ),
         :ok <-
           equal_field(
             :operation_class,
             request.operation_class,
             normalized_operation_class(descriptor)
           ),
         :ok <-
           equal_field(
             :credential_scope_ref,
             attr(descriptor, :credential_scope_ref),
             attr(dependency, :credential_scope_ref)
           ),
         :ok <- validate_digest(descriptor, dependency),
         :ok <- validate_side_effect(descriptor, dependency),
         do: validate_required_scopes(descriptor, dependency)
  end

  defp validate_authority_material(attrs, descriptor, binding_descriptor) do
    with :ok <- require_authority_packet(attrs),
         :ok <- require_credential_lease(attrs, descriptor, binding_descriptor),
         do: require_confirmation_policy(attrs, descriptor, binding_descriptor)
  end

  defp require_authority_packet(attrs) do
    if present?(attr(attrs, :authority_decision_ref) || attr(attrs, :authority_packet_ref)) do
      :ok
    else
      {:error, :missing_authority_decision}
    end
  end

  defp require_credential_lease(attrs, descriptor, binding_descriptor) do
    required? =
      present?(attr(descriptor, :credential_scope_ref)) or
        present?(attr(binding_descriptor, :credential_binding_ref))

    if required? and not present?(attr(attrs, :credential_lease_ref)) do
      {:error, :missing_credential_lease}
    else
      :ok
    end
  end

  defp require_confirmation_policy(attrs, descriptor, binding_descriptor) do
    side_effect_class = normalize_atom(attr(descriptor, :side_effect_class))

    policy_ref =
      attr(attrs, :confirmation_policy_ref) || attr(binding_descriptor, :confirmation_policy_ref)

    if side_effect_class in @side_effecting_classes and not present?(policy_ref) do
      {:error, {:missing_confirmation_policy, side_effect_class}}
    else
      :ok
    end
  end

  defp validate_digest(descriptor, dependency) do
    descriptor_digest = attr(descriptor, :manifest_digest)
    dependency_digest = attr(dependency, :manifest_digest)

    cond do
      is_nil(descriptor_digest) or is_nil(dependency_digest) -> :ok
      descriptor_digest == dependency_digest -> :ok
      true -> {:error, {:manifest_digest_mismatch, descriptor_digest, dependency_digest}}
    end
  end

  defp validate_side_effect(descriptor, dependency) do
    descriptor_class = normalize_atom(attr(descriptor, :side_effect_class))
    dependency_class = normalize_atom(attr(dependency, :side_effect_class))

    cond do
      is_nil(dependency_class) -> :ok
      descriptor_class == dependency_class -> :ok
      true -> {:error, {:side_effect_class_expanded, descriptor_class, dependency_class}}
    end
  end

  defp validate_required_scopes(descriptor, dependency) do
    descriptor_scopes =
      descriptor
      |> attr(:required_scopes)
      |> normalized_list()

    dependency_scopes =
      dependency
      |> attr(:required_scopes)
      |> normalized_list()

    expanded_scopes = descriptor_scopes -- dependency_scopes

    case expanded_scopes do
      [] -> :ok
      scopes -> {:error, {:required_scope_expansion, scopes}}
    end
  end

  defp equal_field(_field, nil, _expected), do: :ok
  defp equal_field(_field, _actual, nil), do: :ok
  defp equal_field(_field, actual, expected) when actual == expected, do: :ok

  defp equal_field(field, actual, expected),
    do: {:error, {:field_mismatch, field, actual, expected}}

  defp normalized_operation_class(map) do
    map
    |> attr(:operation_class)
    |> normalize_atom()
  end

  defp lookup_role(map, role) when is_map(map) do
    Map.get(map, role) ||
      Map.get(map, to_string(role)) ||
      Map.get(map, normalize_atom(role))
  end

  defp lookup_role(_map, _role), do: nil

  defp normalized_list(nil), do: []
  defp normalized_list(list) when is_list(list), do: Enum.map(list, &to_string/1)
  defp normalized_list(value), do: [to_string(value)]

  defp normalize_atom(nil), do: nil
  defp normalize_atom(value) when is_atom(value), do: value

  defp normalize_atom(value) when is_binary(value) do
    Enum.find(@known_atoms, value, &(Atom.to_string(&1) == value)) || value
  end

  defp normalize_metadata(nil), do: %{}
  defp normalize_metadata(metadata) when is_map(metadata), do: metadata
  defp normalize_metadata(_metadata), do: %{}

  defp required_map(attrs, field) do
    case attr(attrs, field) do
      value when is_map(value) -> {:ok, value}
      nil -> {:error, {:missing_required_binding_resolver_attr, field}}
      value -> {:error, {:invalid_binding_resolver_attr, field, value}}
    end
  end

  defp required_attr(attrs, field) do
    case attr(attrs, field) do
      value when is_binary(value) -> required_binary(field, value)
      value when is_atom(value) -> {:ok, value}
      value when is_integer(value) -> {:ok, value}
      nil -> {:error, {:missing_required_binding_resolver_attr, field}}
      value -> {:error, {:invalid_binding_resolver_attr, field, value}}
    end
  end

  defp required_binary(field, value) do
    if String.trim(value) == "" do
      {:error, {:blank_binding_resolver_attr, field}}
    else
      {:ok, value}
    end
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}
  defp normalize_attrs(attrs) when is_map(attrs), do: {:ok, attrs}
  defp normalize_attrs(_attrs), do: {:error, :invalid_binding_resolver_attrs}

  defp attr(attrs, key) when is_map(attrs) do
    Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
  end

  defp attr(attrs, key) when is_list(attrs), do: Keyword.get(attrs, key)
  defp attr(_attrs, _key), do: nil

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)

  defp fail_closed(reason) do
    %Error{
      reason: reason,
      retryable: retryable?(reason),
      recovery_owner: recovery_owner(reason),
      blast_radius: :single_invocation,
      propagation_target: propagation_target(reason),
      operator_action: operator_action(reason),
      details: details(reason)
    }
  end

  defp retryable?({:stale_binding_epoch, _expected, _actual}), do: false
  defp retryable?({:manifest_digest_mismatch, _actual, _expected}), do: false
  defp retryable?(:missing_credential_lease), do: true
  defp retryable?(:missing_authority_decision), do: true
  defp retryable?(_reason), do: false

  defp recovery_owner(:missing_credential_lease), do: :credential_authority
  defp recovery_owner(:missing_authority_decision), do: :authority_boundary
  defp recovery_owner({:missing_confirmation_policy, _class}), do: :product_pack_owner
  defp recovery_owner({:required_scope_expansion, _scopes}), do: :authority_boundary
  defp recovery_owner({:side_effect_class_expanded, _actual, _expected}), do: :authority_boundary
  defp recovery_owner(_reason), do: :platform_operator

  defp propagation_target(:missing_credential_lease), do: :credential_lease
  defp propagation_target(:missing_authority_decision), do: :authority_decision
  defp propagation_target({:missing_confirmation_policy, _class}), do: :confirmation_policy
  defp propagation_target({:stale_binding_epoch, _expected, _actual}), do: :binding_registry
  defp propagation_target({:manifest_digest_mismatch, _actual, _expected}), do: :manifest_registry
  defp propagation_target({:required_scope_expansion, _scopes}), do: :authority_policy

  defp propagation_target({:side_effect_class_expanded, _actual, _expected}),
    do: :authority_policy

  defp propagation_target(_reason), do: :binding_resolver

  defp operator_action(:missing_credential_lease), do: :rematerialize_credential_lease
  defp operator_action(:missing_authority_decision), do: :rerun_authority_decision
  defp operator_action({:missing_confirmation_policy, _class}), do: :add_confirmation_policy

  defp operator_action({:stale_binding_epoch, _expected, _actual}),
    do: :restart_with_current_binding_epoch

  defp operator_action({:manifest_digest_mismatch, _actual, _expected}),
    do: :refresh_manifest_descriptor

  defp operator_action({:required_scope_expansion, _scopes}), do: :review_authority_scope_policy

  defp operator_action({:side_effect_class_expanded, _actual, _expected}),
    do: :review_side_effect_policy

  defp operator_action(_reason), do: :inspect_binding_resolution

  defp details({:stale_binding_epoch, expected, actual}) do
    %{expected_binding_epoch: expected, actual_binding_epoch: actual}
  end

  defp details({:required_scope_expansion, scopes}), do: %{expanded_scopes: scopes}
  defp details(_reason), do: %{}
end
