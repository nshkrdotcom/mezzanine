defmodule Mezzanine.ConfigRegistry.BindingManifestDependency do
  @moduledoc """
  Manifest operation dependency declared by a compiled binding.
  """

  use Ash.Resource,
    domain: Mezzanine.ConfigRegistry,
    data_layer: AshPostgres.DataLayer

  @binding_kinds [
    :source,
    :source_publication,
    :runtime,
    :runtime_tool,
    :evidence,
    :resource_effect
  ]

  postgres do
    table("binding_manifest_dependencies")
    repo(Mezzanine.ConfigRegistry.Repo)

    identity_index_names(
      unique_binding_manifest_operation: "binding_manifest_deps_unique_operation"
    )
  end

  code_interface do
    define(:register, action: :register)
    define(:get, action: :read)
    define(:by_set, action: :by_set, args: [:binding_set_id])
    define(:by_binding, action: :by_binding, args: [:compiled_binding_id])

    define(:by_binding_role,
      action: :by_binding_role,
      args: [:compiled_binding_id, :operation_role]
    )
  end

  actions do
    defaults([:read])

    create :register do
      accept([
        :binding_set_id,
        :compiled_binding_id,
        :binding_ref,
        :binding_kind,
        :connector_ref,
        :manifest_ref,
        :operation_role,
        :operation_ref,
        :operation_class,
        :side_effect_class,
        :credential_scope_ref,
        :required_runtime_family,
        :manifest_digest,
        :required_scopes,
        :metadata
      ])
    end

    read :by_set do
      argument(:binding_set_id, :uuid, allow_nil?: false)
      filter(expr(binding_set_id == ^arg(:binding_set_id)))
    end

    read :by_binding do
      argument(:compiled_binding_id, :uuid, allow_nil?: false)
      filter(expr(compiled_binding_id == ^arg(:compiled_binding_id)))
    end

    read :by_binding_role do
      argument(:compiled_binding_id, :uuid, allow_nil?: false)
      argument(:operation_role, :string, allow_nil?: false)
      get?(true)

      filter(
        expr(
          compiled_binding_id == ^arg(:compiled_binding_id) and
            operation_role == ^arg(:operation_role)
        )
      )
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :binding_set_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :compiled_binding_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :binding_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :binding_kind, :atom do
      allow_nil?(false)
      constraints(one_of: @binding_kinds)
      public?(true)
    end

    attribute :connector_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :manifest_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :operation_role, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :operation_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :operation_class, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :side_effect_class, :string do
      public?(true)
    end

    attribute :credential_scope_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :required_runtime_family, :string do
      public?(true)
    end

    attribute :manifest_digest, :string do
      public?(true)
    end

    attribute :required_scopes, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :metadata, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    timestamps()
  end

  identities do
    identity(:unique_binding_manifest_operation, [
      :compiled_binding_id,
      :operation_role,
      :operation_ref
    ])
  end
end
