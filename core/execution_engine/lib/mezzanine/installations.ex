defmodule Mezzanine.Installations do
  @moduledoc """
  Neutral installation bootstrap helpers for runtime-profile provisioning and
  durable routing-context resolution.
  """

  alias Mezzanine.Programs.{PlacementProfile, PolicyBundle, Program}
  alias Mezzanine.ServiceSupport
  alias Mezzanine.Work.WorkClass

  @type ensure_status :: :unchanged | :updated
  @type program_record :: struct()
  @type policy_bundle_record :: struct()
  @type work_class_record :: struct()
  @type placement_profile_record :: struct()
  @type runtime_profile_resolution :: %{
          status: ensure_status(),
          program: program_record() | nil,
          policy_bundle: policy_bundle_record() | nil,
          work_class: work_class_record() | nil,
          placement_profile: placement_profile_record() | nil
        }

  @spec ensure_runtime_profile(String.t(), map() | nil) ::
          {:ok, runtime_profile_resolution()} | {:error, term()}
  def ensure_runtime_profile(_tenant_id, nil) do
    {:ok,
     %{
       status: :unchanged,
       program: nil,
       policy_bundle: nil,
       work_class: nil,
       placement_profile: nil
     }}
  end

  def ensure_runtime_profile(tenant_id, runtime_profile)
      when is_binary(tenant_id) and is_map(runtime_profile) do
    with {:ok, normalized_profile} <- normalize_profile(runtime_profile),
         {:ok, program, program_changed?} <- ensure_program(tenant_id, normalized_profile.program),
         {:ok, policy_bundle, policy_changed?} <-
           ensure_policy_bundle(tenant_id, program, normalized_profile.policy_bundle),
         {:ok, work_class, work_class_changed?} <-
           ensure_work_class(
             tenant_id,
             program,
             normalized_profile.work_class,
             policy_bundle
           ),
         {:ok, placement_profile, placement_changed?} <-
           ensure_placement_profile(
             tenant_id,
             program,
             normalized_profile.placement_profile
           ) do
      {:ok,
       %{
         status:
           if(program_changed? or policy_changed? or work_class_changed? or placement_changed?,
             do: :updated,
             else: :unchanged
           ),
         program: program,
         policy_bundle: policy_bundle,
         work_class: work_class,
         placement_profile: placement_profile
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  def ensure_runtime_profile(_tenant_id, _runtime_profile),
    do: {:error, :invalid_runtime_profile}

  @spec resolve_program_context(String.t(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def resolve_program_context(tenant_id, attrs, opts \\ [])
      when is_binary(tenant_id) and is_map(attrs) and is_list(opts) do
    attrs = Map.new(attrs)

    with {:ok, program_slug} <-
           ServiceSupport.fetch_string(attrs, opts, :program_slug, :missing_program_slug),
         {:ok, program} <- resolve_program(tenant_id, program_slug) do
      work_class_context(
        program,
        tenant_id,
        ServiceSupport.optional_string(attrs, opts, :work_class_name)
      )
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp work_class_context(%Program{} = program, _tenant_id, value)
       when not is_binary(value) or value == "" do
    {:ok, %{program_id: program.id}}
  end

  defp work_class_context(%Program{} = program, tenant_id, work_class_name) do
    case resolve_work_class(tenant_id, program.id, work_class_name) do
      {:ok, work_class_id} -> {:ok, %{program_id: program.id, work_class_id: work_class_id}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp resolve_program(tenant_id, program_slug) do
    case Program.by_slug(tenant_id, program_slug,
           actor: actor(tenant_id),
           tenant: tenant_id
         ) do
      {:ok, %Program{} = program} -> {:ok, program}
      {:error, _reason} -> {:error, :not_found}
    end
  end

  defp resolve_work_class(tenant_id, program_id, work_class_name) do
    case WorkClass.list_for_program(program_id, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, work_classes} ->
        case Enum.find(work_classes, &(&1.name == work_class_name)) do
          %WorkClass{id: work_class_id} -> {:ok, work_class_id}
          nil -> {:error, :not_found}
        end

      {:error, _reason} ->
        {:error, :not_found}
    end
  end

  defp ensure_program(tenant_id, attrs) do
    case Program.by_slug(tenant_id, attrs.slug, actor: actor(tenant_id), tenant: tenant_id) do
      {:ok, %Program{} = program} ->
        update_program_if_needed(tenant_id, program, attrs)

      _other ->
        with {:ok, program} <-
               Program.create_program(
                 %{
                   slug: attrs.slug,
                   name: attrs.name,
                   product_family: attrs.product_family,
                   configuration: attrs.configuration,
                   metadata: attrs.metadata
                 },
                 actor: actor(tenant_id),
                 tenant: tenant_id
               ),
             {:ok, active_program} <- ensure_active_program(tenant_id, program) do
          {:ok, active_program, true}
        end
    end
  end

  defp update_program_if_needed(tenant_id, %Program{} = program, attrs) do
    if program_matches?(program, attrs) do
      {:ok, program, false}
    else
      with {:ok, updated_program} <-
             Program.update_program(
               program,
               %{
                 name: attrs.name,
                 product_family: attrs.product_family,
                 configuration: attrs.configuration,
                 metadata: attrs.metadata
               },
               actor: actor(tenant_id),
               tenant: tenant_id
             ),
           {:ok, active_program} <- ensure_active_program(tenant_id, updated_program) do
        {:ok, active_program, true}
      end
    end
  end

  defp ensure_active_program(_tenant_id, %Program{status: :active} = program),
    do: {:ok, program}

  defp ensure_active_program(tenant_id, %Program{} = program) do
    Program.activate(program, actor: actor(tenant_id), tenant: tenant_id)
  end

  defp ensure_policy_bundle(tenant_id, %Program{} = program, attrs) do
    with {:ok, policy_bundles} <-
           PolicyBundle.list_for_program(program.id, actor: actor(tenant_id), tenant: tenant_id) do
      case Enum.find(policy_bundles, &(&1.name == attrs.name)) do
        nil ->
          create_policy_bundle(tenant_id, program.id, attrs)

        %PolicyBundle{} = policy_bundle ->
          update_policy_bundle_if_needed(tenant_id, policy_bundle, attrs)
      end
    end
  end

  defp update_policy_bundle_if_needed(
         _tenant_id,
         %PolicyBundle{policy_kind: existing_kind},
         %{policy_kind: desired_kind}
       )
       when existing_kind != desired_kind,
       do: {:error, :unsupported_runtime_profile_change}

  defp update_policy_bundle_if_needed(tenant_id, %PolicyBundle{} = policy_bundle, attrs) do
    if policy_bundle_matches?(policy_bundle, attrs) do
      {:ok, policy_bundle, false}
    else
      with {:ok, recompiled_bundle} <-
             PolicyBundle.recompile(
               policy_bundle,
               %{
                 version: attrs.version,
                 source_ref: attrs.source_ref,
                 body: attrs.body,
                 metadata: attrs.metadata
               },
               actor: actor(tenant_id),
               tenant: tenant_id
             ) do
        {:ok, recompiled_bundle, true}
      end
    end
  end

  defp create_policy_bundle(tenant_id, program_id, attrs) do
    with {:ok, policy_bundle} <-
           PolicyBundle.load_bundle(
             %{
               program_id: program_id,
               name: attrs.name,
               version: attrs.version,
               policy_kind: attrs.policy_kind,
               source_ref: attrs.source_ref,
               body: attrs.body,
               metadata: attrs.metadata
             },
             actor: actor(tenant_id),
             tenant: tenant_id
           ) do
      {:ok, policy_bundle, true}
    end
  end

  defp ensure_work_class(tenant_id, %Program{} = program, attrs, %PolicyBundle{} = policy_bundle) do
    with {:ok, work_classes} <-
           WorkClass.list_for_program(program.id, actor: actor(tenant_id), tenant: tenant_id) do
      case Enum.find(work_classes, &(&1.name == attrs.name)) do
        nil ->
          create_work_class(tenant_id, program.id, attrs, policy_bundle.id)

        %WorkClass{} = work_class ->
          update_work_class_if_needed(tenant_id, work_class, attrs, policy_bundle)
      end
    end
  end

  defp update_work_class_if_needed(
         tenant_id,
         %WorkClass{} = work_class,
         attrs,
         %PolicyBundle{} = policy_bundle
       ) do
    if work_class_matches?(work_class, attrs, policy_bundle.id) do
      {:ok, work_class, false}
    else
      work_class
      |> Ash.Changeset.for_update(:update_work_class, %{
        name: attrs.name,
        kind: attrs.kind,
        intake_schema: attrs.intake_schema,
        policy_bundle_id: policy_bundle.id,
        default_review_profile: attrs.default_review_profile,
        default_run_profile: attrs.default_run_profile,
        status: :active
      })
      |> Ash.Changeset.set_tenant(tenant_id)
      |> Ash.update(actor: actor(tenant_id), domain: Mezzanine.Work)
      |> case do
        {:ok, updated_work_class} -> {:ok, updated_work_class, true}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp create_work_class(tenant_id, program_id, attrs, policy_bundle_id) do
    with {:ok, work_class} <-
           WorkClass.create_work_class(
             %{
               program_id: program_id,
               name: attrs.name,
               kind: attrs.kind,
               intake_schema: attrs.intake_schema,
               policy_bundle_id: policy_bundle_id,
               default_review_profile: attrs.default_review_profile,
               default_run_profile: attrs.default_run_profile
             },
             actor: actor(tenant_id),
             tenant: tenant_id
           ) do
      {:ok, work_class, true}
    end
  end

  defp ensure_placement_profile(tenant_id, %Program{} = program, attrs) do
    with {:ok, placement_profiles} <-
           PlacementProfile.list_for_program(program.id,
             actor: actor(tenant_id),
             tenant: tenant_id
           ) do
      case Enum.find(placement_profiles, &(&1.profile_id == attrs.profile_id)) do
        nil ->
          create_placement_profile(tenant_id, program.id, attrs)

        %PlacementProfile{} = placement_profile ->
          update_placement_profile_if_needed(tenant_id, placement_profile, attrs)
      end
    end
  end

  defp update_placement_profile_if_needed(
         tenant_id,
         %PlacementProfile{} = placement_profile,
         attrs
       ) do
    if placement_profile_matches?(placement_profile, attrs) do
      {:ok, placement_profile, false}
    else
      with {:ok, updated_profile} <-
             PlacementProfile.update_profile(
               placement_profile,
               %{
                 strategy: attrs.strategy,
                 target_selector: attrs.target_selector,
                 runtime_preferences: attrs.runtime_preferences,
                 workspace_policy: attrs.workspace_policy,
                 metadata: attrs.metadata
               },
               actor: actor(tenant_id),
               tenant: tenant_id
             ),
           {:ok, active_profile} <- ensure_active_placement_profile(tenant_id, updated_profile) do
        {:ok, active_profile, true}
      end
    end
  end

  defp ensure_active_placement_profile(
         _tenant_id,
         %PlacementProfile{status: :active} = profile
       ),
       do: {:ok, profile}

  defp ensure_active_placement_profile(tenant_id, %PlacementProfile{} = profile) do
    PlacementProfile.activate(profile, actor: actor(tenant_id), tenant: tenant_id)
  end

  defp create_placement_profile(tenant_id, program_id, attrs) do
    with {:ok, placement_profile} <-
           PlacementProfile.create_profile(
             %{
               program_id: program_id,
               profile_id: attrs.profile_id,
               strategy: attrs.strategy,
               target_selector: attrs.target_selector,
               runtime_preferences: attrs.runtime_preferences,
               workspace_policy: attrs.workspace_policy,
               metadata: attrs.metadata
             },
             actor: actor(tenant_id),
             tenant: tenant_id
           ),
         {:ok, active_profile} <- ensure_active_placement_profile(tenant_id, placement_profile) do
      {:ok, active_profile, true}
    end
  end

  defp normalize_profile(runtime_profile) do
    with {:ok, program} <- normalize_program(required_map(runtime_profile, :program)),
         {:ok, policy_bundle} <-
           normalize_policy_bundle(required_map(runtime_profile, :policy_bundle)),
         {:ok, work_class} <- normalize_work_class(required_map(runtime_profile, :work_class)),
         {:ok, placement_profile} <-
           normalize_placement_profile(required_map(runtime_profile, :placement_profile)) do
      {:ok,
       %{
         program: program,
         policy_bundle: policy_bundle,
         work_class: work_class,
         placement_profile: placement_profile
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp normalize_program({:ok, attrs}) do
    with {:ok, slug} <- required_string(attrs, :slug),
         {:ok, name} <- required_string(attrs, :name) do
      {:ok,
       %{
         slug: slug,
         name: name,
         product_family: optional_string(attrs, :product_family),
         configuration: canonical_map(ServiceSupport.map_value(attrs, :configuration) || %{}),
         metadata: canonical_map(ServiceSupport.map_value(attrs, :metadata) || %{})
       }}
    end
  end

  defp normalize_program(_attrs), do: {:error, :invalid_runtime_profile}

  defp normalize_policy_bundle({:ok, attrs}) do
    with {:ok, name} <- required_string(attrs, :name),
         {:ok, version} <- required_string(attrs, :version),
         {:ok, body} <- required_string(attrs, :body),
         {:ok, policy_kind} <-
           normalize_policy_kind(ServiceSupport.map_value(attrs, :policy_kind)) do
      {:ok,
       %{
         name: name,
         version: version,
         body: body,
         policy_kind: policy_kind,
         source_ref:
           optional_string(attrs, :source_ref) ||
             "app_kit/runtime_profile/#{name}",
         metadata: canonical_map(ServiceSupport.map_value(attrs, :metadata) || %{})
       }}
    end
  end

  defp normalize_policy_bundle(_attrs), do: {:error, :invalid_runtime_profile}

  defp normalize_work_class({:ok, attrs}) do
    with {:ok, name} <- required_string(attrs, :name),
         {:ok, kind} <- required_string(attrs, :kind) do
      {:ok,
       %{
         name: name,
         kind: kind,
         intake_schema: canonical_map(ServiceSupport.map_value(attrs, :intake_schema) || %{}),
         default_review_profile:
           canonical_map(ServiceSupport.map_value(attrs, :default_review_profile) || %{}),
         default_run_profile:
           canonical_map(ServiceSupport.map_value(attrs, :default_run_profile) || %{})
       }}
    end
  end

  defp normalize_work_class(_attrs), do: {:error, :invalid_runtime_profile}

  defp normalize_placement_profile({:ok, attrs}) do
    with {:ok, profile_id} <- required_string(attrs, :profile_id),
         {:ok, strategy} <- required_string(attrs, :strategy) do
      {:ok,
       %{
         profile_id: profile_id,
         strategy: strategy,
         target_selector: canonical_map(ServiceSupport.map_value(attrs, :target_selector) || %{}),
         runtime_preferences:
           canonical_map(ServiceSupport.map_value(attrs, :runtime_preferences) || %{}),
         workspace_policy:
           canonical_map(ServiceSupport.map_value(attrs, :workspace_policy) || %{}),
         metadata: canonical_map(ServiceSupport.map_value(attrs, :metadata) || %{})
       }}
    end
  end

  defp normalize_placement_profile(_attrs), do: {:error, :invalid_runtime_profile}

  defp required_map(attrs, key) do
    case ServiceSupport.map_value(attrs, key) do
      value when is_map(value) -> {:ok, value}
      _ -> {:error, :invalid_runtime_profile}
    end
  end

  defp required_string(attrs, key) do
    case ServiceSupport.map_value(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, :invalid_runtime_profile}
    end
  end

  defp optional_string(attrs, key) do
    case ServiceSupport.map_value(attrs, key) do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  defp normalize_policy_kind(value) when value in [:workflow_md, :structured_config, :inline],
    do: {:ok, value}

  defp normalize_policy_kind("workflow_md"), do: {:ok, :workflow_md}
  defp normalize_policy_kind("structured_config"), do: {:ok, :structured_config}
  defp normalize_policy_kind("inline"), do: {:ok, :inline}
  defp normalize_policy_kind(nil), do: {:ok, :workflow_md}
  defp normalize_policy_kind(_value), do: {:error, :invalid_runtime_profile}

  defp program_matches?(%Program{} = program, attrs) do
    program.name == attrs.name and
      program.product_family == attrs.product_family and
      canonical_map(program.configuration) == attrs.configuration and
      canonical_map(program.metadata) == attrs.metadata and
      program.status == :active
  end

  defp policy_bundle_matches?(%PolicyBundle{} = policy_bundle, attrs) do
    policy_bundle.name == attrs.name and
      policy_bundle.version == attrs.version and
      policy_bundle.policy_kind == attrs.policy_kind and
      policy_bundle.source_ref == attrs.source_ref and
      policy_bundle.body == attrs.body and
      canonical_map(policy_bundle.metadata) == attrs.metadata and
      policy_bundle.status == :compiled
  end

  defp work_class_matches?(%WorkClass{} = work_class, attrs, policy_bundle_id) do
    work_class.name == attrs.name and
      work_class.kind == attrs.kind and
      canonical_map(work_class.intake_schema) == attrs.intake_schema and
      work_class.policy_bundle_id == policy_bundle_id and
      canonical_map(work_class.default_review_profile) == attrs.default_review_profile and
      canonical_map(work_class.default_run_profile) == attrs.default_run_profile and
      work_class.status == :active
  end

  defp placement_profile_matches?(%PlacementProfile{} = placement_profile, attrs) do
    placement_profile.profile_id == attrs.profile_id and
      placement_profile.strategy == attrs.strategy and
      canonical_map(placement_profile.target_selector) == attrs.target_selector and
      canonical_map(placement_profile.runtime_preferences) == attrs.runtime_preferences and
      canonical_map(placement_profile.workspace_policy) == attrs.workspace_policy and
      canonical_map(placement_profile.metadata) == attrs.metadata and
      placement_profile.status == :active
  end

  defp canonical_map(%_{} = value), do: value |> Map.from_struct() |> canonical_map()

  defp canonical_map(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested_value} ->
      {canonical_key(key), canonical_map(nested_value)}
    end)
    |> Enum.sort()
    |> Map.new()
  end

  defp canonical_map(value) when is_list(value), do: Enum.map(value, &canonical_map/1)
  defp canonical_map(value), do: value

  defp canonical_key(key) when is_atom(key), do: Atom.to_string(key)
  defp canonical_key(key), do: key

  defp actor(tenant_id), do: ServiceSupport.actor(tenant_id)
end
