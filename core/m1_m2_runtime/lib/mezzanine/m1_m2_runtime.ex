defmodule Mezzanine.M1M2Runtime do
  @moduledoc """
  Ref-only M1/M2 runtime separation contracts.
  """

  alias Mezzanine.WorkflowRuntime.TemporalSupervisor

  defmodule Receipt do
    @moduledoc """
    Ref-only M1/M2 runtime admission receipt.
    """

    @type t :: %__MODULE__{
            mode: :m1 | :m2,
            fixture_ref: String.t() | nil,
            projection_ref: String.t() | nil,
            provider_account_ref: String.t() | nil,
            connector_instance_ref: String.t() | nil,
            connector_binding_ref: String.t() | nil,
            credential_lease_ref: String.t() | nil,
            target_ref: String.t() | nil,
            attach_grant_ref: String.t() | nil,
            operation_policy_ref: String.t() | nil,
            runtime_substrate_ref: String.t() | nil,
            trace_ref: String.t() | nil,
            persistence_profile: atom() | nil,
            substrate_preflight_ref: String.t() | nil,
            live_provider_call?: boolean(),
            credential_materialized?: boolean(),
            temporal_worker_used?: boolean()
          }

    defstruct [
      :mode,
      :fixture_ref,
      :projection_ref,
      :provider_account_ref,
      :connector_instance_ref,
      :connector_binding_ref,
      :credential_lease_ref,
      :target_ref,
      :attach_grant_ref,
      :operation_policy_ref,
      :runtime_substrate_ref,
      :trace_ref,
      :persistence_profile,
      :substrate_preflight_ref,
      live_provider_call?: false,
      credential_materialized?: false,
      temporal_worker_used?: false
    ]
  end

  @m1_forbidden_capabilities [
    :live_provider,
    :live_connector,
    :temporal_worker,
    :credential_materializer,
    :postgres_shared,
    :temporal_durable,
    :object_store,
    :local_restart_safe
  ]

  @m2_required_refs [
    :provider_account_ref,
    :connector_instance_ref,
    :connector_binding_ref,
    :credential_lease_ref,
    :target_ref,
    :attach_grant_ref,
    :operation_policy_ref,
    :runtime_substrate_ref,
    :persistence_profile
  ]

  @m2_required_capabilities [:temporal_durable]

  @known_fields [
                  :mode,
                  :fixture_ref,
                  :projection_ref,
                  :capabilities,
                  :trace_ref,
                  :persistence_profile,
                  :substrate_preflight_ref,
                  :temporal_address,
                  :temporal_namespace,
                  :temporal_substrate_available?
                ] ++
                  @m2_required_refs

  @spec admit(map() | keyword()) ::
          {:ok, Receipt.t()}
          | {:error, {:m1_forbidden_capabilities, [atom()]}}
          | {:error, {:m2_missing_required_refs, [atom()]}}
          | {:error, {:m2_missing_substrate_capabilities, [atom()]}}
          | {:error, {:m2_unsupported_persistence_profile, atom() | term()}}
          | {:error, {:temporal_substrate_unavailable, map()}}
          | {:error, :invalid_runtime_mode}
  def admit(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)

    case Map.get(attrs, :mode) do
      mode when mode in [:m1, "m1"] -> admit_m1(attrs)
      mode when mode in [:m2, "m2"] -> admit_m2(attrs)
      _mode -> {:error, :invalid_runtime_mode}
    end
  end

  defp admit_m1(attrs) do
    capabilities = List.wrap(Map.get(attrs, :capabilities, []))

    forbidden =
      capabilities
      |> Enum.filter(&(&1 in @m1_forbidden_capabilities))
      |> Kernel.++(forbidden_persistence_profiles(attrs))

    case forbidden do
      [] ->
        {:ok,
         %Receipt{
           mode: :m1,
           fixture_ref: Map.get(attrs, :fixture_ref),
           projection_ref: Map.get(attrs, :projection_ref),
           persistence_profile: Map.get(attrs, :persistence_profile)
         }}

      values ->
        {:error, {:m1_forbidden_capabilities, values}}
    end
  end

  defp forbidden_persistence_profiles(attrs) do
    profile_ref = Map.get(attrs, :persistence_profile)

    case Mezzanine.Persistence.resolve(profile: profile_ref) do
      {:ok, profile} when profile.durable? -> [profile.id]
      {:ok, _profile} -> []
      {:error, _reason} -> []
    end
  end

  defp admit_m2(attrs) do
    missing = Enum.reject(@m2_required_refs, &present?(Map.get(attrs, &1)))

    case missing do
      [] ->
        with :ok <- require_m2_temporal_profile(attrs),
             :ok <- require_m2_temporal_capability(attrs),
             :ok <- temporal_preflight(attrs) do
          {:ok,
           %Receipt{
             mode: :m2,
             provider_account_ref: Map.fetch!(attrs, :provider_account_ref),
             connector_instance_ref: Map.fetch!(attrs, :connector_instance_ref),
             connector_binding_ref: Map.fetch!(attrs, :connector_binding_ref),
             credential_lease_ref: Map.fetch!(attrs, :credential_lease_ref),
             target_ref: Map.fetch!(attrs, :target_ref),
             attach_grant_ref: Map.fetch!(attrs, :attach_grant_ref),
             operation_policy_ref: Map.fetch!(attrs, :operation_policy_ref),
             runtime_substrate_ref: Map.fetch!(attrs, :runtime_substrate_ref),
             trace_ref: Map.get(attrs, :trace_ref),
             persistence_profile: Map.fetch!(attrs, :persistence_profile),
             substrate_preflight_ref: Map.get(attrs, :substrate_preflight_ref),
             live_provider_call?: true,
             temporal_worker_used?: true
           }}
        end

      values ->
        {:error, {:m2_missing_required_refs, values}}
    end
  end

  defp require_m2_temporal_profile(attrs) do
    profile_ref = Map.fetch!(attrs, :persistence_profile)

    case Mezzanine.Persistence.resolve(profile: profile_ref) do
      {:ok, %{default_tier: :temporal_durable}} ->
        :ok

      {:ok, %{id: profile_id}} ->
        {:error, {:m2_unsupported_persistence_profile, profile_id}}

      {:error, reason} ->
        {:error, {:m2_unsupported_persistence_profile, reason}}
    end
  end

  defp require_m2_temporal_capability(attrs) do
    capabilities = List.wrap(Map.get(attrs, :capabilities, []))
    missing = Enum.reject(@m2_required_capabilities, &(&1 in capabilities))

    case missing do
      [] -> :ok
      values -> {:error, {:m2_missing_substrate_capabilities, values}}
    end
  end

  defp temporal_preflight(attrs) do
    TemporalSupervisor.preflight(
      enabled?: true,
      address: Map.get(attrs, :temporal_address, "127.0.0.1:7233"),
      namespace: Map.get(attrs, :temporal_namespace, "default"),
      substrate_available?: Map.get(attrs, :temporal_substrate_available?, false)
    )
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {string_key(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp string_key(key), do: Enum.find(@known_fields, key, &same_key?(&1, key))
  defp same_key?(field, key), do: Atom.to_string(field) == key

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
