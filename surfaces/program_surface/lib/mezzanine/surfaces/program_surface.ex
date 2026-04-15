defmodule Mezzanine.Surfaces.ProgramSurface do
  @moduledoc """
  Reusable northbound administration surface for programs, policies, work
  classes, and placement profiles.
  """

  require Ash.Query

  alias Mezzanine.Programs.{PlacementProfile, PolicyBundle, Program}
  alias Mezzanine.Work.WorkClass

  @opaque program_resource :: struct()
  @opaque policy_bundle_resource :: struct()
  @opaque work_class_resource :: struct()
  @opaque placement_profile_resource :: struct()

  @spec program_id(program_resource()) :: Ecto.UUID.t()
  def program_id(program), do: Map.fetch!(program, :id)

  @spec program_slug(program_resource()) :: String.t()
  def program_slug(program), do: Map.fetch!(program, :slug)

  @spec program_name(program_resource()) :: String.t()
  def program_name(program), do: Map.fetch!(program, :name)

  @spec program_status(program_resource()) :: atom()
  def program_status(program), do: Map.fetch!(program, :status)

  @spec policy_bundle_id(policy_bundle_resource()) :: Ecto.UUID.t()
  def policy_bundle_id(policy_bundle), do: Map.fetch!(policy_bundle, :id)

  @spec policy_bundle_name(policy_bundle_resource()) :: String.t()
  def policy_bundle_name(policy_bundle), do: Map.fetch!(policy_bundle, :name)

  @spec work_class_id(work_class_resource()) :: Ecto.UUID.t()
  def work_class_id(work_class), do: Map.fetch!(work_class, :id)

  @spec work_class_name(work_class_resource()) :: String.t()
  def work_class_name(work_class), do: Map.fetch!(work_class, :name)

  @spec placement_profile_id(placement_profile_resource()) :: Ecto.UUID.t()
  def placement_profile_id(placement_profile), do: Map.fetch!(placement_profile, :id)

  @spec placement_profile_profile_id(placement_profile_resource()) :: String.t()
  def placement_profile_profile_id(placement_profile),
    do: Map.fetch!(placement_profile, :profile_id)

  @spec placement_profile_status(placement_profile_resource()) :: atom()
  def placement_profile_status(placement_profile), do: Map.fetch!(placement_profile, :status)

  @spec create_program(String.t(), map()) :: {:ok, program_resource()} | {:error, term()}
  def create_program(tenant_id, attrs) when is_binary(tenant_id) and is_map(attrs) do
    Program.create_program(Map.new(attrs), actor: actor(tenant_id), tenant: tenant_id)
  end

  @spec update_program(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, program_resource()} | {:error, term()}
  def update_program(tenant_id, program_id, attrs)
      when is_binary(tenant_id) and is_binary(program_id) and is_map(attrs) do
    with {:ok, program} <- fetch_program(tenant_id, program_id) do
      Program.update_program(program, Map.new(attrs), actor: actor(tenant_id), tenant: tenant_id)
    end
  end

  @spec activate_program(String.t(), Ecto.UUID.t()) ::
          {:ok, program_resource()} | {:error, term()}
  def activate_program(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, program} <- fetch_program(tenant_id, program_id) do
      Program.activate(program, actor: actor(tenant_id), tenant: tenant_id)
    end
  end

  @spec suspend_program(String.t(), Ecto.UUID.t()) ::
          {:ok, program_resource()} | {:error, term()}
  def suspend_program(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    with {:ok, program} <- fetch_program(tenant_id, program_id) do
      Program.suspend(program, actor: actor(tenant_id), tenant: tenant_id)
    end
  end

  @spec list_programs(String.t(), map()) :: {:ok, [program_resource()]} | {:error, term()}
  def list_programs(tenant_id, filters \\ %{}) when is_binary(tenant_id) and is_map(filters) do
    Program.list_for_tenant(tenant_id, actor: actor(tenant_id), tenant: tenant_id)
    |> case do
      {:ok, programs} -> {:ok, Enum.filter(programs, &matches_filters?(&1, filters))}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec load_policy_bundle(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, policy_bundle_resource()} | {:error, term()}
  def load_policy_bundle(tenant_id, program_id, attrs)
      when is_binary(tenant_id) and is_binary(program_id) and is_map(attrs) do
    attrs = Map.put(Map.new(attrs), :program_id, program_id)
    PolicyBundle.load_bundle(attrs, actor: actor(tenant_id), tenant: tenant_id)
  end

  @spec recompile_policy_bundle(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, policy_bundle_resource()} | {:error, term()}
  def recompile_policy_bundle(tenant_id, policy_bundle_id, attrs)
      when is_binary(tenant_id) and is_binary(policy_bundle_id) and is_map(attrs) do
    with {:ok, policy_bundle} <- fetch_policy_bundle(tenant_id, policy_bundle_id) do
      PolicyBundle.recompile(policy_bundle, Map.new(attrs),
        actor: actor(tenant_id),
        tenant: tenant_id
      )
    end
  end

  @spec list_policy_bundles(String.t(), Ecto.UUID.t()) ::
          {:ok, [policy_bundle_resource()]} | {:error, term()}
  def list_policy_bundles(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    PolicyBundle.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id)
  end

  @spec create_work_class(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, work_class_resource()} | {:error, term()}
  def create_work_class(tenant_id, program_id, attrs)
      when is_binary(tenant_id) and is_binary(program_id) and is_map(attrs) do
    attrs = Map.put(Map.new(attrs), :program_id, program_id)
    WorkClass.create_work_class(attrs, actor: actor(tenant_id), tenant: tenant_id)
  end

  @spec update_work_class(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, work_class_resource()} | {:error, term()}
  def update_work_class(tenant_id, work_class_id, attrs)
      when is_binary(tenant_id) and is_binary(work_class_id) and is_map(attrs) do
    with {:ok, work_class} <- fetch_work_class(tenant_id, work_class_id) do
      work_class
      |> Ash.Changeset.for_update(:update_work_class, Map.new(attrs))
      |> Ash.Changeset.set_tenant(tenant_id)
      |> Ash.update(actor: actor(tenant_id), domain: Mezzanine.Work)
    end
  end

  @spec list_work_classes(String.t(), Ecto.UUID.t()) ::
          {:ok, [work_class_resource()]} | {:error, term()}
  def list_work_classes(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    WorkClass.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id)
  end

  @spec create_placement_profile(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, placement_profile_resource()} | {:error, term()}
  def create_placement_profile(tenant_id, program_id, attrs)
      when is_binary(tenant_id) and is_binary(program_id) and is_map(attrs) do
    attrs = Map.put(Map.new(attrs), :program_id, program_id)
    PlacementProfile.create_profile(attrs, actor: actor(tenant_id), tenant: tenant_id)
  end

  @spec update_placement_profile(String.t(), Ecto.UUID.t(), map()) ::
          {:ok, placement_profile_resource()} | {:error, term()}
  def update_placement_profile(tenant_id, placement_profile_id, attrs)
      when is_binary(tenant_id) and is_binary(placement_profile_id) and is_map(attrs) do
    with {:ok, placement_profile} <- fetch_placement_profile(tenant_id, placement_profile_id) do
      PlacementProfile.update_profile(
        placement_profile,
        Map.new(attrs),
        actor: actor(tenant_id),
        tenant: tenant_id
      )
    end
  end

  @spec activate_placement_profile(String.t(), Ecto.UUID.t()) ::
          {:ok, placement_profile_resource()} | {:error, term()}
  def activate_placement_profile(tenant_id, placement_profile_id)
      when is_binary(tenant_id) and is_binary(placement_profile_id) do
    with {:ok, placement_profile} <- fetch_placement_profile(tenant_id, placement_profile_id) do
      PlacementProfile.activate(placement_profile, actor: actor(tenant_id), tenant: tenant_id)
    end
  end

  @spec retire_placement_profile(String.t(), Ecto.UUID.t()) ::
          {:ok, placement_profile_resource()} | {:error, term()}
  def retire_placement_profile(tenant_id, placement_profile_id)
      when is_binary(tenant_id) and is_binary(placement_profile_id) do
    with {:ok, placement_profile} <- fetch_placement_profile(tenant_id, placement_profile_id) do
      PlacementProfile.retire(placement_profile, actor: actor(tenant_id), tenant: tenant_id)
    end
  end

  @spec list_placement_profiles(String.t(), Ecto.UUID.t()) ::
          {:ok, [placement_profile_resource()]} | {:error, term()}
  def list_placement_profiles(tenant_id, program_id)
      when is_binary(tenant_id) and is_binary(program_id) do
    PlacementProfile.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id)
  end

  defp fetch_program(tenant_id, program_id) do
    Program
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^program_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Programs)
    |> case do
      {:ok, [program]} -> {:ok, program}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_policy_bundle(tenant_id, policy_bundle_id) do
    PolicyBundle
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^policy_bundle_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Programs)
    |> case do
      {:ok, [policy_bundle]} -> {:ok, policy_bundle}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_work_class(tenant_id, work_class_id) do
    WorkClass
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^work_class_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Work)
    |> case do
      {:ok, [work_class]} -> {:ok, work_class}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_placement_profile(tenant_id, placement_profile_id) do
    PlacementProfile
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^placement_profile_id)
    |> Ash.read(actor: actor(tenant_id), domain: Mezzanine.Programs)
    |> case do
      {:ok, [placement_profile]} -> {:ok, placement_profile}
      {:ok, []} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp matches_filters?(program, filters) do
    status = Map.get(filters, :status) || Map.get(filters, "status")
    family = Map.get(filters, :product_family) || Map.get(filters, "product_family")

    (is_nil(status) or program.status == status) and
      (is_nil(family) or program.product_family == family)
  end

  defp actor(tenant_id), do: %{tenant_id: tenant_id}
end
