defmodule Mezzanine.Authoring.Bundle do
  @moduledoc """
  Deterministic internal/operator bundle envelope for pack import.

  Bundles are validation artifacts, not code-loading artifacts. They carry a
  serialized pack manifest plus descriptor metadata and are rejected before
  registration or runtime activation when any descriptor, policy, lifecycle
  hint, checksum, signature, or platform-migration rule fails.
  """

  alias Mezzanine.ConfigRegistry.LifecycleHintContract
  alias Mezzanine.Pack.{CompiledPack, Compiler, Serializer}

  @type issue :: %{
          required(:code) => atom(),
          required(:path) => [String.t()],
          required(:message) => String.t(),
          optional(:details) => term()
        }

  @type t :: %__MODULE__{
          bundle_id: String.t(),
          tenant_id: String.t(),
          installation_id: String.t(),
          pack_manifest: map(),
          lifecycle_specs: [map()],
          decision_specs: [map()],
          binding_descriptors: map(),
          observer_descriptors: [map()],
          context_adapter_descriptors: [map()],
          policy_refs: [String.t()],
          expected_installation_revision: non_neg_integer() | nil,
          checksum: String.t(),
          signature: String.t() | nil,
          authored_by: String.t(),
          compiled_pack: CompiledPack.t()
        }

  defstruct [
    :bundle_id,
    :tenant_id,
    :installation_id,
    :pack_manifest,
    :lifecycle_specs,
    :decision_specs,
    :binding_descriptors,
    :observer_descriptors,
    :context_adapter_descriptors,
    :expected_installation_revision,
    :checksum,
    :signature,
    :authored_by,
    :compiled_pack,
    policy_refs: []
  ]

  @required_fields [
    "bundle_id",
    "tenant_id",
    "installation_id",
    "pack_manifest",
    "lifecycle_specs",
    "decision_specs",
    "binding_descriptors",
    "observer_descriptors",
    "context_adapter_descriptors",
    "checksum",
    "authored_by"
  ]

  @platform_migration_fields ["platform_migrations", "schema_migrations", "migrations"]
  @checksum_prefix "sha256:"
  @signature_prefix "hmac-sha256:"

  @spec new(map(), keyword()) :: {:ok, t()} | {:error, {:invalid_authoring_bundle, [issue()]}}
  def new(attrs, opts \\ []) when is_map(attrs) do
    normalized_attrs = normalize_map(attrs)
    {compiled_pack, manifest_issues} = compile_manifest(normalized_attrs)

    issues =
      []
      |> require_fields(normalized_attrs)
      |> reject_platform_migrations(normalized_attrs)
      |> validate_policy_refs(normalized_attrs, opts)
      |> validate_manifest_echo(normalized_attrs)
      |> validate_binding_descriptors(normalized_attrs, compiled_pack)
      |> validate_context_adapter_descriptors(normalized_attrs, opts)
      |> validate_observer_descriptors(normalized_attrs)
      |> Kernel.++(manifest_issues)
      |> validate_checksum(normalized_attrs)
      |> validate_signature(normalized_attrs, opts)

    case issues do
      [] -> {:ok, build_bundle(normalized_attrs, compiled_pack)}
      issues -> {:error, {:invalid_authoring_bundle, issues}}
    end
  end

  @spec checksum_for(map()) :: String.t()
  def checksum_for(attrs) when is_map(attrs) do
    digest =
      attrs
      |> normalize_map()
      |> Map.drop(["checksum", "signature"])
      |> canonical_binary()
      |> then(&:crypto.hash(:sha256, &1))

    @checksum_prefix <> Base.encode16(digest, case: :lower)
  end

  @spec signature_for(map(), String.t()) :: String.t()
  def signature_for(attrs, signing_key) when is_map(attrs) and is_binary(signing_key) do
    signature =
      :crypto.mac(:hmac, :sha256, signing_key, checksum_for(attrs))
      |> Base.encode16(case: :lower)

    @signature_prefix <> signature
  end

  defp build_bundle(attrs, %CompiledPack{} = compiled_pack) do
    %__MODULE__{
      bundle_id: attrs["bundle_id"],
      tenant_id: attrs["tenant_id"],
      installation_id: attrs["installation_id"],
      pack_manifest: attrs["pack_manifest"],
      lifecycle_specs: attrs["lifecycle_specs"],
      decision_specs: attrs["decision_specs"],
      binding_descriptors: attrs["binding_descriptors"],
      observer_descriptors: attrs["observer_descriptors"],
      context_adapter_descriptors: attrs["context_adapter_descriptors"],
      policy_refs: normalize_string_list(attrs["policy_refs"] || []),
      expected_installation_revision: attrs["expected_installation_revision"],
      checksum: attrs["checksum"],
      signature: attrs["signature"],
      authored_by: attrs["authored_by"],
      compiled_pack: compiled_pack
    }
  end

  defp require_fields(issues, attrs) do
    @required_fields
    |> Enum.reduce(issues, fn field, acc ->
      if blank?(Map.get(attrs, field)) do
        [issue(:missing_field, [field], "required authoring bundle field is missing") | acc]
      else
        acc
      end
    end)
    |> Enum.reverse()
  end

  defp reject_platform_migrations(issues, attrs) do
    @platform_migration_fields
    |> Enum.reduce(issues, fn field, acc ->
      if nonempty?(Map.get(attrs, field)) do
        [
          issue(
            :pack_authored_platform_migration,
            [field],
            "authoring bundles may not carry platform table migrations"
          )
          | acc
        ]
      else
        acc
      end
    end)
    |> Enum.reverse()
  end

  defp validate_policy_refs(issues, attrs, opts) do
    allowed_refs = Keyword.get(opts, :allowed_policy_refs)

    if is_list(allowed_refs) do
      allowed = allowed_refs |> normalize_string_list() |> MapSet.new()

      attrs
      |> Map.get("policy_refs", [])
      |> normalize_string_list()
      |> Enum.reject(&MapSet.member?(allowed, &1))
      |> case do
        [] ->
          issues

        invalid_refs ->
          [
            issue(
              :invalid_policy_ref,
              ["policy_refs"],
              "authoring bundle references policy refs that are not allowed",
              invalid_refs
            )
            | issues
          ]
      end
    else
      issues
    end
  end

  defp validate_manifest_echo(issues, attrs) do
    manifest = map_or_empty(attrs["pack_manifest"])

    issues
    |> validate_echo_list("lifecycle_specs", manifest["lifecycle_specs"] || [], attrs)
    |> validate_echo_list("decision_specs", manifest["decision_specs"] || [], attrs)
  end

  defp validate_echo_list(issues, field, manifest_value, attrs) do
    value = Map.get(attrs, field)

    cond do
      not is_list(value) ->
        [issue(:invalid_schema, [field], "bundle field must be a list") | issues]

      canonicalize(value) != canonicalize(manifest_value) ->
        [
          issue(
            echo_mismatch_kind(field),
            [field],
            "bundle field must match the serialized pack manifest"
          )
          | issues
        ]

      true ->
        issues
    end
  end

  defp validate_binding_descriptors(issues, attrs, nil) do
    if is_map(attrs["binding_descriptors"]) do
      issues
    else
      [
        issue(
          :invalid_binding_descriptors,
          ["binding_descriptors"],
          "binding descriptors must be a map"
        )
        | issues
      ]
    end
  end

  defp validate_binding_descriptors(issues, attrs, %CompiledPack{} = compiled_pack) do
    binding_descriptors = map_or_empty(attrs["binding_descriptors"])
    execution_bindings = map_or_empty(binding_descriptors["execution_bindings"])

    recipe_refs =
      compiled_pack.manifest.execution_recipe_specs
      |> Enum.map(&to_string(&1.recipe_ref))

    missing_bindings =
      recipe_refs
      |> Enum.reject(&Map.has_key?(execution_bindings, &1))

    issues =
      if missing_bindings == [] do
        issues
      else
        [
          issue(
            :missing_execution_binding,
            ["binding_descriptors", "execution_bindings"],
            "every execution recipe must have an execution binding descriptor",
            missing_bindings
          )
          | issues
        ]
      end

    case LifecycleHintContract.violations(compiled_pack, binding_descriptors) do
      [] ->
        issues

      violations ->
        [
          issue(
            :missing_lifecycle_hints,
            ["binding_descriptors", "execution_bindings"],
            "execution binding connector capabilities do not produce required lifecycle hints",
            violations
          )
          | issues
        ]
    end
  end

  defp validate_context_adapter_descriptors(issues, attrs, opts) do
    descriptors = attrs["context_adapter_descriptors"]
    registry = context_adapter_registry(opts)
    binding_config = map_or_empty(attrs["binding_descriptors"])
    context_bindings = map_or_empty(binding_config["context_bindings"])

    if is_list(descriptors) do
      Enum.reduce(descriptors, issues, fn descriptor, acc ->
        validate_context_adapter_descriptor(acc, descriptor, registry, context_bindings)
      end)
    else
      [
        issue(
          :invalid_context_adapter_descriptors,
          ["context_adapter_descriptors"],
          "context adapter descriptors must be a list"
        )
        | issues
      ]
    end
  end

  defp validate_context_adapter_descriptor(issues, descriptor, registry, context_bindings)
       when is_map(descriptor) do
    binding_key = descriptor["binding_key"]
    adapter_key = descriptor["adapter_key"]

    issues =
      cond do
        blank?(binding_key) ->
          [
            issue(
              :invalid_context_adapter_descriptor,
              ["context_adapter_descriptors", "binding_key"],
              "context adapter descriptor requires a binding_key"
            )
            | issues
          ]

        not Map.has_key?(context_bindings, binding_key) ->
          [
            issue(
              :missing_context_binding,
              ["context_adapter_descriptors", binding_key],
              "context adapter descriptor must reference a declared context binding"
            )
            | issues
          ]

        true ->
          issues
      end

    if map_size(registry) > 0 and not Map.has_key?(registry, adapter_key) do
      [
        issue(
          :unknown_context_adapter,
          ["context_adapter_descriptors", to_string(adapter_key || "")],
          "context adapter descriptor references an adapter outside the trusted registry"
        )
        | issues
      ]
    else
      issues
    end
  end

  defp validate_context_adapter_descriptor(issues, _descriptor, _registry, _context_bindings) do
    [
      issue(
        :invalid_context_adapter_descriptor,
        ["context_adapter_descriptors"],
        "context adapter descriptor must be a map"
      )
      | issues
    ]
  end

  defp echo_mismatch_kind("lifecycle_specs"), do: :lifecycle_specs_mismatch
  defp echo_mismatch_kind("decision_specs"), do: :decision_specs_mismatch
  defp echo_mismatch_kind(_field), do: :manifest_echo_mismatch

  defp validate_observer_descriptors(issues, attrs) do
    descriptors = attrs["observer_descriptors"]

    observer_bindings =
      map_or_empty(map_or_empty(attrs["binding_descriptors"])["observer_bindings"])

    if is_list(descriptors) do
      Enum.reduce(descriptors, issues, fn descriptor, acc ->
        validate_observer_descriptor(acc, descriptor, observer_bindings)
      end)
    else
      [
        issue(
          :invalid_observer_descriptors,
          ["observer_descriptors"],
          "observer descriptors must be a list"
        )
        | issues
      ]
    end
  end

  defp validate_observer_descriptor(issues, descriptor, observer_bindings)
       when is_map(descriptor) do
    binding_key = descriptor["binding_key"]
    subscriber_key = descriptor["subscriber_key"]

    issues =
      cond do
        blank?(binding_key) ->
          [
            issue(
              :invalid_observer_descriptor,
              ["observer_descriptors", "binding_key"],
              "observer descriptor requires a binding_key"
            )
            | issues
          ]

        map_size(observer_bindings) > 0 and not Map.has_key?(observer_bindings, binding_key) ->
          [
            issue(
              :missing_observer_binding,
              ["observer_descriptors", binding_key],
              "observer descriptor must reference a declared observer binding"
            )
            | issues
          ]

        true ->
          issues
      end

    cond do
      blank?(subscriber_key) ->
        [
          issue(
            :invalid_observer_descriptor,
            ["observer_descriptors", "subscriber_key"],
            "observer descriptor requires a subscriber_key"
          )
          | issues
        ]

      invalid_event_types?(descriptor["event_types"]) ->
        [
          issue(
            :invalid_observer_descriptor,
            ["observer_descriptors", "event_types"],
            "observer descriptor event_types must be a list of non-empty strings"
          )
          | issues
        ]

      true ->
        issues
    end
  end

  defp validate_observer_descriptor(issues, _descriptor, _observer_bindings) do
    [
      issue(
        :invalid_observer_descriptor,
        ["observer_descriptors"],
        "observer descriptor must be a map"
      )
      | issues
    ]
  end

  defp validate_checksum(issues, attrs) do
    expected = checksum_for(attrs)

    cond do
      blank?(attrs["checksum"]) ->
        [issue(:missing_checksum, ["checksum"], "authoring bundle checksum is required") | issues]

      attrs["checksum"] != expected ->
        [
          issue(
            :checksum_mismatch,
            ["checksum"],
            "authoring bundle checksum does not match canonical payload",
            %{expected: expected, actual: attrs["checksum"]}
          )
          | issues
        ]

      true ->
        issues
    end
  end

  defp validate_signature(issues, attrs, opts) do
    case signing_key(opts) do
      nil ->
        issues

      key ->
        expected = signature_for(attrs, key)

        cond do
          blank?(attrs["signature"]) ->
            [
              issue(
                :missing_signature,
                ["signature"],
                "authoring bundle signature is required when a signing key is configured"
              )
              | issues
            ]

          attrs["signature"] != expected ->
            [
              issue(
                :signature_mismatch,
                ["signature"],
                "authoring bundle signature does not match canonical payload",
                %{expected: expected, actual: attrs["signature"]}
              )
              | issues
            ]

          true ->
            issues
        end
    end
  end

  defp compile_manifest(attrs) do
    case Serializer.deserialize_manifest(map_or_empty(attrs["pack_manifest"])) do
      {:ok, manifest} ->
        case Compiler.compile(manifest) do
          {:ok, %CompiledPack{} = compiled_pack} ->
            {compiled_pack, []}

          {:error, errors} ->
            {nil,
             [
               issue(
                 :invalid_pack_manifest,
                 ["pack_manifest"],
                 "pack manifest failed pack compilation",
                 errors
               )
             ]}
        end

      {:error, reason} ->
        {nil,
         [
           issue(
             :invalid_pack_manifest,
             ["pack_manifest"],
             "pack manifest payload is invalid",
             reason
           )
         ]}
    end
  end

  defp context_adapter_registry(opts) do
    opts
    |> Keyword.get(:context_adapter_registry, %{})
    |> normalize_map()
  end

  defp signing_key(opts) do
    opts
    |> Keyword.get_lazy(:signing_key, fn ->
      Application.get_env(:mezzanine_config_registry, :authoring_signing_key)
    end)
    |> case do
      key when is_binary(key) and key != "" -> key
      _other -> nil
    end
  end

  defp issue(code, path, message, details \\ nil) do
    %{code: code, path: Enum.map(path, &to_string/1), message: message, details: details}
  end

  defp blank?(nil), do: true
  defp blank?(""), do: true
  defp blank?(_value), do: false

  defp nonempty?(nil), do: false
  defp nonempty?(""), do: false
  defp nonempty?([]), do: false
  defp nonempty?(value) when is_map(value), do: map_size(value) > 0
  defp nonempty?(_value), do: true

  defp invalid_event_types?(nil), do: false

  defp invalid_event_types?(event_types) when is_list(event_types) do
    Enum.any?(event_types, fn event_type ->
      not is_binary(event_type) or event_type == ""
    end)
  end

  defp invalid_event_types?(_event_types), do: true

  defp map_or_empty(value) when is_map(value), do: value
  defp map_or_empty(_value), do: %{}

  defp normalize_string_list(values) when is_list(values) do
    values
    |> Enum.map(&to_string/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp normalize_string_list(_values), do: []

  defp normalize_map(value) when is_map(value) do
    Map.new(value, fn {key, nested} ->
      {to_string(key), normalize_value(nested)}
    end)
  end

  defp normalize_map(_value), do: %{}

  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_boolean(value) or is_nil(value), do: value
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp canonical_binary(value) do
    value
    |> canonicalize()
    |> :erlang.term_to_binary()
  end

  defp canonicalize(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), canonicalize(nested)} end)
    |> Enum.sort_by(fn {key, _value} -> key end)
  end

  defp canonicalize(value) when is_list(value), do: Enum.map(value, &canonicalize/1)
  defp canonicalize(value) when is_boolean(value) or is_nil(value), do: value
  defp canonicalize(value) when is_atom(value), do: Atom.to_string(value)
  defp canonicalize(value), do: value
end
