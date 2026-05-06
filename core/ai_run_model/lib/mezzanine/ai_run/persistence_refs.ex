defmodule Mezzanine.AIRun.PersistenceRefs do
  @moduledoc """
  Persistence profile refs for AI run envelopes.

  Defaults are memory-only and carry no restart-safety claim.
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

  defmodule BootPosture do
    @moduledoc "Default boot posture for memory-only AI run paths."
    @enforce_keys [
      :profile_id,
      :store_tiers,
      :disabled_substrates,
      :restart_safe?,
      :debug_capture?
    ]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            profile_id: atom(),
            store_tiers: [atom()],
            disabled_substrates: [atom()],
            restart_safe?: boolean(),
            debug_capture?: boolean()
          }
  end

  @durable_profiles [:local_restart_safe, :integration_postgres, :ops_durable]
  @debug_profiles [:memory_debug, :full_debug_tracked]

  @spec default_profile() :: Profile.t()
  def default_profile do
    %Profile{
      id: :mickey_mouse,
      store_category: :ai_run_envelope,
      selected_tier: :memory_ephemeral,
      capture_level: :minimal_refs,
      store_ref: "store://memory/ai_run_envelope",
      partition_ref: "partition://memory/default",
      retention_ref: "retention://memory/session",
      debug_tap_ref: nil,
      restart_safe?: false,
      durable?: false
    }
  end

  @spec memory_default_boot() :: BootPosture.t()
  def memory_default_boot do
    %BootPosture{
      profile_id: :mickey_mouse,
      store_tiers: [:memory_ephemeral],
      disabled_substrates: [:postgres, :temporal, :object_store, :debug_sidecar],
      restart_safe?: false,
      debug_capture?: false
    }
  end

  @spec resolve(nil | map() | Profile.t()) :: {:ok, Profile.t()} | {:error, term()}
  def resolve(nil), do: {:ok, default_profile()}
  def resolve(%Profile{} = profile), do: validate_resolved(profile)

  def resolve(profile) when is_map(profile) do
    id = get(profile, :id) || :mickey_mouse

    if unavailable_durable?(id, profile) do
      {:error, {:durable_profile_unavailable, id}}
    else
      profile
      |> resolved_profile(id)
      |> validate_resolved()
    end
  end

  def resolve(_profile), do: {:error, :invalid_persistence_profile_ref}

  defp validate_resolved(%Profile{id: id} = profile) when is_atom(id), do: {:ok, profile}
  defp validate_resolved(_profile), do: {:error, :invalid_persistence_profile_ref}

  defp unavailable_durable?(id, profile) do
    durable_profile?(id) and get(profile, :available?) == false
  end

  defp resolved_profile(profile, id) do
    selected_tier = get(profile, :selected_tier) || default_selected_tier(id)

    %Profile{
      id: id,
      store_category: get(profile, :store_category) || :ai_run_envelope,
      selected_tier: selected_tier,
      capture_level: get(profile, :capture_level) || default_capture_level(id),
      store_ref: get(profile, :store_ref) || default_store_ref(selected_tier),
      partition_ref: get(profile, :partition_ref) || "partition://memory/default",
      retention_ref: get(profile, :retention_ref) || default_retention_ref(id),
      debug_tap_ref: get(profile, :debug_tap_ref),
      restart_safe?: durable_profile?(id),
      durable?: durable_profile?(id)
    }
  end

  defp durable_profile?(id), do: id in @durable_profiles
  defp debug_profile?(id), do: id in @debug_profiles

  defp default_selected_tier(id) do
    if durable_profile?(id), do: :durable_explicit, else: :memory_ephemeral
  end

  defp default_capture_level(id) do
    if debug_profile?(id), do: :debug_redacted, else: :minimal_refs
  end

  defp default_store_ref(:memory_ephemeral), do: "store://memory/ai_run_envelope"
  defp default_store_ref(selected_tier), do: "store://#{selected_tier}/ai_run_envelope"

  defp default_retention_ref(id) do
    if durable_profile?(id), do: "retention://explicit", else: "retention://memory/session"
  end

  defp get(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end
end
