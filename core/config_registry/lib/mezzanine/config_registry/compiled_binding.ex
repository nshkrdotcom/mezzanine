defmodule Mezzanine.ConfigRegistry.CompiledBinding do
  @moduledoc """
  Materialized binding descriptor for one binding ref in a binding set.
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
    table("compiled_bindings")
    repo(Mezzanine.ConfigRegistry.Repo)
  end

  code_interface do
    define(:register, action: :register)
    define(:get, action: :read)
    define(:by_set, action: :by_set, args: [:binding_set_id])
    define(:by_set_ref, action: :by_set_ref, args: [:binding_set_id, :binding_ref])
  end

  actions do
    defaults([:read])

    create :register do
      accept([
        :binding_set_id,
        :binding_ref,
        :binding_kind,
        :connector_ref,
        :manifest_ref,
        :credential_binding_ref,
        :runtime_family,
        :operation_refs,
        :binding_payload,
        :metadata
      ])
    end

    read :by_set do
      argument(:binding_set_id, :uuid, allow_nil?: false)
      filter(expr(binding_set_id == ^arg(:binding_set_id)))
    end

    read :by_set_ref do
      argument(:binding_set_id, :uuid, allow_nil?: false)
      argument(:binding_ref, :string, allow_nil?: false)
      get?(true)
      filter(expr(binding_set_id == ^arg(:binding_set_id) and binding_ref == ^arg(:binding_ref)))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :binding_set_id, :uuid do
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

    attribute :credential_binding_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :runtime_family, :string do
      public?(true)
    end

    attribute :operation_refs, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :binding_payload, :map do
      allow_nil?(false)
      default(%{})
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
    identity(:unique_compiled_binding_ref, [:binding_set_id, :binding_ref])
  end
end
