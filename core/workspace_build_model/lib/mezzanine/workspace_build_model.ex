defmodule Mezzanine.WorkspaceBuildModel do
  @moduledoc """
  Workspace-to-runtime build manifests with no-secret bundle checks.
  """

  defmodule Manifest do
    @moduledoc """
    Ref-only workspace build manifest for runtime handoff.
    """

    @type t :: %__MODULE__{
            workspace_ref: String.t(),
            agent_refs: [String.t()],
            role_refs: [String.t()],
            trigger_refs: [String.t()],
            provider_account_refs: [String.t()],
            connector_binding_refs: [String.t()],
            target_posture_refs: [String.t()],
            env_contract_refs: [String.t()],
            secret_contract_refs: [String.t()],
            plugin_boundary_refs: [String.t()],
            runtime_invocation_ref: String.t(),
            projection_ref: String.t(),
            manifest_ref: String.t(),
            raw_material_present?: false
          }

    defstruct [
      :workspace_ref,
      :agent_refs,
      :role_refs,
      :trigger_refs,
      :provider_account_refs,
      :connector_binding_refs,
      :target_posture_refs,
      :env_contract_refs,
      :secret_contract_refs,
      :plugin_boundary_refs,
      :runtime_invocation_ref,
      :projection_ref,
      :manifest_ref,
      raw_material_present?: false
    ]
  end

  @required_refs [
    :workspace_ref,
    :agent_refs,
    :role_refs,
    :trigger_refs,
    :provider_account_refs,
    :connector_binding_refs,
    :target_posture_refs,
    :env_contract_refs,
    :secret_contract_refs,
    :plugin_boundary_refs,
    :runtime_invocation_ref,
    :projection_ref,
    :manifest_ref
  ]

  @forbidden_material [
    :api_key,
    :auth_json,
    :local_auth_path,
    :raw_local_path_state,
    :raw_secret,
    :raw_token,
    :token_file,
    :unmanaged_workspace_auth
  ]

  @known_fields @required_refs ++ @forbidden_material

  @spec build_manifest(map() | keyword()) ::
          {:ok, Manifest.t()}
          | {:error, {:missing_workspace_build_refs, [atom()]}}
          | {:error, {:forbidden_workspace_build_material, [atom()]}}
  def build_manifest(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize(attrs)

    case forbidden_material(attrs) do
      [] ->
        case missing_refs(attrs) do
          [] -> {:ok, manifest(attrs)}
          missing -> {:error, {:missing_workspace_build_refs, missing}}
        end

      forbidden ->
        {:error, {:forbidden_workspace_build_material, forbidden}}
    end
  end

  defp manifest(attrs) do
    %Manifest{
      workspace_ref: Map.fetch!(attrs, :workspace_ref),
      agent_refs: List.wrap(Map.fetch!(attrs, :agent_refs)),
      role_refs: List.wrap(Map.fetch!(attrs, :role_refs)),
      trigger_refs: List.wrap(Map.fetch!(attrs, :trigger_refs)),
      provider_account_refs: List.wrap(Map.fetch!(attrs, :provider_account_refs)),
      connector_binding_refs: List.wrap(Map.fetch!(attrs, :connector_binding_refs)),
      target_posture_refs: List.wrap(Map.fetch!(attrs, :target_posture_refs)),
      env_contract_refs: List.wrap(Map.fetch!(attrs, :env_contract_refs)),
      secret_contract_refs: List.wrap(Map.fetch!(attrs, :secret_contract_refs)),
      plugin_boundary_refs: List.wrap(Map.fetch!(attrs, :plugin_boundary_refs)),
      runtime_invocation_ref: Map.fetch!(attrs, :runtime_invocation_ref),
      projection_ref: Map.fetch!(attrs, :projection_ref),
      manifest_ref: Map.fetch!(attrs, :manifest_ref)
    }
  end

  defp forbidden_material(attrs), do: Enum.filter(@forbidden_material, &Map.has_key?(attrs, &1))
  defp missing_refs(attrs), do: Enum.reject(@required_refs, &present?(Map.get(attrs, &1)))

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {string_key(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp string_key(key), do: Enum.find(@known_fields, key, &(Atom.to_string(&1) == key))

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_list(value), do: value != []
  defp present?(value), do: not is_nil(value)
end
