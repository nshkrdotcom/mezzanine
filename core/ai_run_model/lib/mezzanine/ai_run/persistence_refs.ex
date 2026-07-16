defmodule Mezzanine.AIRun.PersistenceRefs do
  @moduledoc """
  Explicit durable persistence profile refs for AI run envelopes.

  A run envelope cannot select a backend by omission. Production callers must
  inject the immutable durable profile chosen by the NSHKR runtime.
  """

  defmodule Profile do
    @moduledoc "Resolved persistence profile ref."
    @enforce_keys [
      :id,
      :store_category,
      :selected_tier,
      :capture_level,
      :store_ref,
      :partition_ref,
      :retention_ref,
      :debug_tap_ref,
      :restart_safe?,
      :durable?
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            id: atom(),
            store_category: atom(),
            selected_tier: atom(),
            capture_level: atom(),
            store_ref: String.t(),
            partition_ref: String.t(),
            retention_ref: String.t(),
            debug_tap_ref: nil | String.t(),
            restart_safe?: boolean(),
            durable?: boolean()
          }
  end

  @durable_profiles [:local_restart_safe, :integration_postgres, :ops_durable]
  @durable_tiers [:postgres_shared, :temporal_postgres]

  @spec production_profile(keyword() | map()) :: {:ok, Profile.t()} | {:error, term()}
  def production_profile(attrs) when is_list(attrs),
    do: attrs |> Map.new() |> production_profile()

  def production_profile(attrs) when is_map(attrs) do
    %Profile{
      id: get(attrs, :id) || :ops_durable,
      store_category: :ai_run_envelope,
      selected_tier: get(attrs, :selected_tier) || :postgres_shared,
      capture_level: get(attrs, :capture_level) || :standard_redacted,
      store_ref: get(attrs, :store_ref) || "store://mezzanine/postgres/ai-run-envelope",
      partition_ref: get(attrs, :partition_ref) || "partition://mezzanine/ai-runs",
      retention_ref: get(attrs, :retention_ref) || "retention://mezzanine/standard",
      debug_tap_ref: get(attrs, :debug_tap_ref),
      restart_safe?: true,
      durable?: true
    }
    |> validate_resolved()
  end

  @spec resolve(nil | map() | Profile.t()) :: {:ok, Profile.t()} | {:error, term()}
  def resolve(nil), do: {:error, :persistence_profile_required}
  def resolve(%Profile{} = profile), do: validate_resolved(profile)

  def resolve(profile) when is_map(profile) do
    id = get(profile, :id)

    if id in @durable_profiles and get(profile, :available?) != false do
      resolved_profile(profile, id) |> validate_resolved()
    else
      {:error, {:durable_profile_unavailable, id}}
    end
  end

  def resolve(_profile), do: {:error, :invalid_persistence_profile_ref}

  defp validate_resolved(%Profile{} = profile) do
    if profile.id in @durable_profiles and profile.selected_tier in @durable_tiers and
         profile.restart_safe? and profile.durable? and safe_ref?(profile.store_ref) and
         safe_ref?(profile.partition_ref) and safe_ref?(profile.retention_ref) do
      {:ok, profile}
    else
      {:error, :invalid_persistence_profile_ref}
    end
  end

  defp validate_resolved(_profile), do: {:error, :invalid_persistence_profile_ref}

  defp resolved_profile(profile, id) do
    %Profile{
      id: id,
      store_category: get(profile, :store_category) || :ai_run_envelope,
      selected_tier: get(profile, :selected_tier) || :postgres_shared,
      capture_level: get(profile, :capture_level) || :standard_redacted,
      store_ref: get(profile, :store_ref) || "store://mezzanine/postgres/ai-run-envelope",
      partition_ref: get(profile, :partition_ref) || "partition://mezzanine/ai-runs",
      retention_ref: get(profile, :retention_ref) || "retention://mezzanine/standard",
      debug_tap_ref: get(profile, :debug_tap_ref),
      restart_safe?: true,
      durable?: true
    }
  end

  defp safe_ref?(value), do: is_binary(value) and String.contains?(value, "://")

  defp get(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end
end
