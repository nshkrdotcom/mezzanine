defmodule Mezzanine.Pack.ManifestOperationValidator do
  @moduledoc false

  alias Mezzanine.Pack.{
    BindingSpec,
    Manifest,
    RuntimeBinding,
    ValidationError
  }

  alias Mezzanine.Pack.Canonicalizer, as: H

  @required_descriptor_fields [
    :connector_ref,
    :manifest_ref,
    :operation_ref,
    :operation_class,
    :binding_kind,
    :side_effect_class,
    :input_schema_ref,
    :output_schema_ref,
    :credential_scope_ref,
    :runtime_family,
    :manifest_digest
  ]

  @default_operation_classes %{
    evidence: :evidence_collection,
    resource_effect: :resource_effect,
    runtime: :runtime_session,
    runtime_tool: :runtime_tool_invocation,
    source: :source_read,
    source_publication: :source_write
  }

  @default_side_effect_classes %{
    evidence: :read,
    resource_effect: :resource_effect,
    runtime: :write,
    runtime_tool: :read,
    source: :read,
    source_publication: :write
  }

  @spec diagnostics(Manifest.t(), keyword()) :: [ValidationError.t()]
  def diagnostics(%Manifest{} = manifest, opts) when is_list(opts) do
    case Keyword.get(opts, :manifest_resolver) do
      nil -> []
      resolver when is_function(resolver, 1) -> validate_bindings(manifest, resolver)
      resolver -> [ValidationError.error([:manifest_resolver], resolver_error(resolver))]
    end
  end

  defp validate_bindings(%Manifest{} = manifest, resolver) do
    manifest.binding_specs
    |> Enum.with_index()
    |> Enum.flat_map(fn {binding, binding_index} ->
      binding
      |> operation_refs()
      |> Enum.with_index()
      |> Enum.flat_map(fn {{role, operation_ref}, role_index} ->
        validate_binding_operation(
          manifest,
          binding,
          binding_index,
          role,
          operation_ref,
          role_index,
          resolver
        )
      end)
    end)
  end

  defp validate_binding_operation(
         %Manifest{} = manifest,
         binding,
         binding_index,
         role,
         operation_ref,
         role_index,
         resolver
       ) do
    request = lookup_request(manifest, binding, role, operation_ref)
    path = [:binding_specs, binding_index, :operation_refs, role_index]

    case safe_resolve(resolver, request) do
      {:ok, descriptor} ->
        validate_descriptor(binding, role, descriptor, request, path)

      {:error, reason} ->
        [
          ValidationError.error(
            path,
            "manifest validation failed for operation #{inspect(operation_ref)}: #{inspect(reason)}"
          )
        ]
    end
  end

  defp lookup_request(%Manifest{} = manifest, binding, role, operation_ref) do
    binding_kind = BindingSpec.kind(binding)

    %{
      connector_ref: H.canonicalize_identifier!(binding.connector_ref),
      manifest_ref: H.canonicalize_identifier!(binding.manifest_ref),
      operation_ref: H.canonicalize_identifier!(operation_ref),
      operation_role: connector_operation_role(binding_kind, role),
      operation_class: operation_class(binding, role, binding_kind),
      binding_kind: binding_kind,
      required_runtime_family: runtime_family(binding),
      binding_ref: H.canonicalize_identifier!(binding.binding_ref),
      pack_ref: H.canonicalize_identifier!(manifest.pack_slug),
      pack_revision: manifest.version,
      credential_scope_ref: H.canonicalize_identifier!(binding.credential_binding_ref),
      compiled_manifest_hash: metadata_scalar(binding, :manifest_digest),
      metadata: %{
        source: :pack_compiler_manifest_validation,
        pack_operation_role: role
      }
    }
  end

  defp connector_operation_role(:source, _role), do: :source_read
  defp connector_operation_role(:source_publication, _role), do: :source_publish
  defp connector_operation_role(:runtime, _role), do: :runtime_session
  defp connector_operation_role(:runtime_tool, _role), do: :runtime_tool
  defp connector_operation_role(:evidence, _role), do: :evidence_collection
  defp connector_operation_role(:resource_effect, _role), do: :resource_effect

  defp safe_resolve(resolver, request) do
    resolver.(request)
  rescue
    error -> {:error, Exception.message(error)}
  end

  defp validate_descriptor(binding, role, descriptor, request, path) do
    required_descriptor_issues(descriptor, path) ++
      descriptor_match_issues(binding, role, descriptor, request, path)
  end

  defp required_descriptor_issues(descriptor, path) do
    @required_descriptor_fields
    |> Enum.reject(&present?(value(descriptor, &1)))
    |> Enum.map(fn field ->
      ValidationError.error(path ++ [field], "manifest descriptor field #{field} is required")
    end)
  end

  defp descriptor_match_issues(binding, role, descriptor, request, path) do
    [
      match_issue(descriptor, :connector_ref, request.connector_ref, path),
      match_issue(descriptor, :manifest_ref, request.manifest_ref, path),
      match_issue(descriptor, :operation_ref, request.operation_ref, path),
      match_issue(descriptor, :operation_class, request.operation_class, path),
      match_issue(descriptor, :binding_kind, request.binding_kind, path),
      match_issue(descriptor, :credential_scope_ref, request.credential_scope_ref, path),
      match_issue(descriptor, :runtime_family, request.required_runtime_family, path),
      match_issue(descriptor, :side_effect_class, side_effect_class(binding, role), path),
      manifest_digest_issue(descriptor, request, path),
      required_scope_issue(binding, role, descriptor, path)
    ]
    |> List.flatten()
  end

  defp match_issue(descriptor, field, expected, path) do
    actual = value(descriptor, field)

    if comparable(actual) == comparable(expected) do
      []
    else
      [
        ValidationError.error(
          path ++ [field],
          "manifest descriptor #{field} expected #{inspect(expected)}, got #{inspect(actual)}"
        )
      ]
    end
  end

  defp manifest_digest_issue(_descriptor, %{compiled_manifest_hash: nil}, _path), do: []

  defp manifest_digest_issue(descriptor, request, path) do
    match_issue(descriptor, :manifest_digest, request.compiled_manifest_hash, path)
  end

  defp required_scope_issue(binding, role, descriptor, path) do
    expected_scopes = metadata_role_value(binding, :required_scopes, role) |> normalize_list()
    actual_scopes = value(descriptor, :required_scopes) |> normalize_list()
    expanded_scopes = actual_scopes -- expected_scopes

    cond do
      expected_scopes == [] ->
        []

      expanded_scopes == [] ->
        []

      true ->
        [
          ValidationError.error(
            path ++ [:required_scopes],
            "manifest descriptor required scopes expanded by #{inspect(expanded_scopes)}"
          )
        ]
    end
  end

  defp operation_refs(binding) do
    binding.operation_refs
    |> Enum.map(fn {role, operation_ref} -> {role, operation_ref} end)
  end

  defp operation_class(binding, role, binding_kind) do
    metadata_role_value(binding, :operation_classes, role) ||
      Map.fetch!(@default_operation_classes, binding_kind)
  end

  defp side_effect_class(binding, role) do
    binding
    |> metadata_role_value(:side_effect_classes, role)
    |> case do
      nil -> Map.fetch!(@default_side_effect_classes, BindingSpec.kind(binding))
      value -> value
    end
  end

  defp runtime_family(%RuntimeBinding{} = binding), do: binding.runtime_family
  defp runtime_family(_binding), do: :direct

  defp metadata_role_value(binding, key, role) do
    binding
    |> metadata_value(key)
    |> case do
      values when is_map(values) -> role_value(values, role)
      value -> value
    end
  end

  defp metadata_scalar(binding, key) do
    case metadata_value(binding, key) do
      value when is_atom(value) -> Atom.to_string(value)
      value when is_binary(value) -> value
      _value -> nil
    end
  end

  defp metadata_value(binding, key) do
    metadata = Map.get(binding, :metadata, %{})
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end

  defp role_value(values, role) do
    Map.get(values, role) || Map.get(values, to_string(role))
  end

  defp normalize_list(nil), do: []
  defp normalize_list(values) when is_list(values), do: Enum.map(values, &to_string/1)
  defp normalize_list(value), do: [to_string(value)]

  defp comparable(value) when is_atom(value), do: Atom.to_string(value)
  defp comparable(value), do: value

  defp value(%_{} = struct, field), do: struct |> Map.from_struct() |> value(field)

  defp value(%{} = map, field) do
    Map.get(map, field) || Map.get(map, Atom.to_string(field))
  end

  defp value(_descriptor, _field), do: nil

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)

  defp resolver_error(resolver) do
    "manifest_resolver must be a one-argument credential-free resolver function, got: #{inspect(resolver)}"
  end
end
