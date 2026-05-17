defmodule Mezzanine.ConfigRegistry.RunBindingSnapshot do
  @moduledoc """
  Run-pinned binding descriptor retained after active bindings advance.
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
    table("run_binding_snapshots")
    repo(Mezzanine.ConfigRegistry.Repo)
  end

  code_interface do
    define(:capture, action: :capture)
    define(:get, action: :read)
    define(:by_snapshot_ref, action: :by_snapshot_ref, args: [:snapshot_ref])

    define(:by_run_binding,
      action: :by_run_binding,
      args: [:tenant_id, :environment, :run_ref, :binding_ref]
    )
  end

  actions do
    defaults([:read])

    create :capture do
      accept([
        :snapshot_ref,
        :tenant_id,
        :environment,
        :pack_slug,
        :run_ref,
        :binding_ref,
        :binding_kind,
        :binding_set_id,
        :compiled_binding_id,
        :binding_epoch,
        :compiled_pack_revision,
        :descriptor,
        :manifest_dependencies,
        :metadata
      ])
    end

    read :by_snapshot_ref do
      argument(:snapshot_ref, :string, allow_nil?: false)
      get?(true)
      filter(expr(snapshot_ref == ^arg(:snapshot_ref)))
    end

    read :by_run_binding do
      argument(:tenant_id, :string, allow_nil?: false)
      argument(:environment, :string, allow_nil?: false)
      argument(:run_ref, :string, allow_nil?: false)
      argument(:binding_ref, :string, allow_nil?: false)
      get?(true)

      filter(
        expr(
          tenant_id == ^arg(:tenant_id) and environment == ^arg(:environment) and
            run_ref == ^arg(:run_ref) and binding_ref == ^arg(:binding_ref)
        )
      )
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :snapshot_ref, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :tenant_id, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :environment, :string do
      allow_nil?(false)
      default("default")
      public?(true)
    end

    attribute :pack_slug, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :run_ref, :string do
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

    attribute :binding_set_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :compiled_binding_id, :uuid do
      allow_nil?(false)
      public?(true)
    end

    attribute :binding_epoch, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :compiled_pack_revision, :integer do
      allow_nil?(false)
      public?(true)
    end

    attribute :descriptor, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :manifest_dependencies, :map do
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
    identity(:unique_binding_snapshot_ref, [:snapshot_ref])
    identity(:unique_run_binding_snapshot, [:tenant_id, :environment, :run_ref, :binding_ref])
  end
end
