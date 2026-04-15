defmodule Mezzanine.Programs.PolicyBundle do
  @moduledoc false
  use Ash.Resource,
    domain: Mezzanine.Programs,
    data_layer: AshPostgres.DataLayer,
    authorizers: [Ash.Policy.Authorizer]

  alias Mezzanine.Programs.Changes.CompilePolicyBundle

  postgres do
    table "policy_bundles"
    repo Mezzanine.OpsDomain.Repo
  end

  code_interface do
    define :load_bundle, action: :load_bundle
    define :list_for_program, action: :list_for_program, args: [:program_id]
    define :recompile, action: :recompile
  end

  actions do
    defaults []

    read :read do
      primary? true
    end

    create :load_bundle do
      accept [:program_id, :name, :version, :policy_kind, :source_ref, :body, :metadata]
      change {CompilePolicyBundle, fallback_on_error?: false}
    end

    read :list_for_program do
      argument :program_id, :uuid, allow_nil?: false
      filter expr(program_id == ^arg(:program_id))
    end

    update :recompile do
      accept [:version, :source_ref, :body, :metadata]
      require_atomic? false
      change {CompilePolicyBundle, fallback_on_error?: true}
    end
  end

  policies do
    policy action_type(:read) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end

    policy action(:load_bundle) do
      authorize_if actor_present()
    end

    policy action(:recompile) do
      authorize_if expr(tenant_id == ^actor(:tenant_id))
    end
  end

  multitenancy do
    strategy :attribute
    attribute :tenant_id
    global? false
  end

  attributes do
    uuid_primary_key :id

    attribute :tenant_id, :string do
      allow_nil? false
      public? true
    end

    attribute :program_id, :uuid do
      allow_nil? false
      public? true
    end

    attribute :name, :string do
      allow_nil? false
      public? true
    end

    attribute :version, :string do
      allow_nil? false
      public? true
    end

    attribute :policy_kind, :atom do
      allow_nil? false
      default :workflow_md
      constraints one_of: [:workflow_md, :structured_config, :inline]
      public? true
    end

    attribute :source_ref, :string do
      allow_nil? false
      default "inline"
      public? true
    end

    attribute :body, :string do
      allow_nil? false
      public? true
    end

    attribute :config, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :prompt_template, :string do
      allow_nil? false
      default ""
      public? true
    end

    attribute :compiled_form, :map do
      allow_nil? false
      default %{}
      public? true
    end

    attribute :status, :atom do
      allow_nil? false
      default :compiled
      constraints one_of: [:compiled, :stale_on_error]
      public? true
    end

    attribute :metadata, :map do
      allow_nil? false
      default %{}
      public? true
    end

    timestamps()
  end

  relationships do
    belongs_to :program, Mezzanine.Programs.Program do
      attribute_type :uuid
      allow_nil? false
      public? true
    end
  end

  identities do
    identity :unique_version_per_program, [:program_id, :name, :version]
  end
end
