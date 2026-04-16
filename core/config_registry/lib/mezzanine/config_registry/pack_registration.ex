defmodule Mezzanine.ConfigRegistry.PackRegistration do
  @moduledoc """
  Durable neutral registry record for a compiled pack artifact.

  Each registration stores the serialized compiled manifest, canonical subject
  kinds, and serializer metadata needed to hydrate installation runtime state.
  """
  require Ash.Query

  use Ash.Resource,
    domain: Mezzanine.ConfigRegistry,
    data_layer: AshPostgres.DataLayer

  postgres do
    table("pack_registrations")
    repo(Mezzanine.ConfigRegistry.Repo)
  end

  code_interface do
    define(:register, action: :register)
    define(:get, action: :read)
    define(:by_slug_version, action: :by_slug_version, args: [:pack_slug, :version])
    define(:list_active, action: :list_active)
    define(:activate, action: :activate)
    define(:deprecate, action: :deprecate)
  end

  actions do
    defaults([:read])

    create :register do
      accept([
        :pack_slug,
        :version,
        :compiled_manifest,
        :canonical_subject_kinds,
        :serializer_version,
        :migration_strategy
      ])

      change(set_attribute(:status, :registered))
    end

    read :by_slug_version do
      argument(:pack_slug, :string, allow_nil?: false)
      argument(:version, :string, allow_nil?: false)
      get?(true)
      filter(expr(pack_slug == ^arg(:pack_slug) and version == ^arg(:version)))
    end

    read :list_active do
      filter(expr(status == :active))
    end

    update :activate do
      accept([])
      require_atomic?(false)

      change(fn changeset, _context ->
        case overlapping_subject_kinds(changeset.data) do
          {:ok, []} ->
            Ash.Changeset.force_change_attribute(changeset, :status, :active)

          {:ok, overlaps} ->
            Ash.Changeset.add_error(
              changeset,
              field: :canonical_subject_kinds,
              message:
                "cannot activate pack registration because canonical subject kinds overlap active packs: #{Enum.join(overlaps, ", ")}"
            )

          {:error, reason} ->
            Ash.Changeset.add_error(
              changeset,
              field: :canonical_subject_kinds,
              message: "failed to validate canonical subject kind overlap: #{inspect(reason)}"
            )
        end
      end)
    end

    update :deprecate do
      accept([])
      require_atomic?(false)
      change(set_attribute(:status, :deprecated))
    end
  end

  attributes do
    uuid_primary_key(:id)

    attribute :pack_slug, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :version, :string do
      allow_nil?(false)
      public?(true)
    end

    attribute :status, :atom do
      allow_nil?(false)
      default(:registered)
      constraints(one_of: [:registered, :active, :deprecated])
      public?(true)
    end

    attribute :compiled_manifest, :map do
      allow_nil?(false)
      default(%{})
      public?(true)
    end

    attribute :canonical_subject_kinds, {:array, :string} do
      allow_nil?(false)
      default([])
      public?(true)
    end

    attribute :serializer_version, :integer do
      allow_nil?(false)
      default(1)
      public?(true)
    end

    attribute :migration_strategy, :string do
      allow_nil?(false)
      default("additive")
      public?(true)
    end

    timestamps()
  end

  relationships do
    has_many :installations, Mezzanine.ConfigRegistry.Installation do
      destination_attribute(:pack_registration_id)
    end
  end

  identities do
    identity(:unique_pack_version, [:pack_slug, :version])
  end

  defp overlapping_subject_kinds(%{id: registration_id, canonical_subject_kinds: subject_kinds}) do
    __MODULE__
    |> Ash.Query.filter(status == :active and id != ^registration_id)
    |> Ash.read(domain: Mezzanine.ConfigRegistry)
    |> case do
      {:ok, active_registrations} ->
        current_subject_kinds = MapSet.new(subject_kinds)

        overlaps =
          active_registrations
          |> Enum.flat_map(fn active_registration ->
            current_subject_kinds
            |> MapSet.intersection(MapSet.new(active_registration.canonical_subject_kinds))
            |> MapSet.to_list()
          end)
          |> Enum.uniq()
          |> Enum.sort()

        {:ok, overlaps}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
