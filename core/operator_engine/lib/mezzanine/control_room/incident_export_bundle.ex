defmodule Mezzanine.ControlRoom.IncidentExportBundle do
  @moduledoc """
  Release-linked incident export artifact contract.

  Export bundles are operator-downloadable evidence envelopes. They carry refs,
  checksums, and redaction manifests only; raw workflow history, lower payloads,
  provider bodies, prompts, artifacts, and tenant-sensitive secrets must stay out
  of the public export shape.
  """

  @contract_name "Mezzanine.IncidentExportBundle.v1"

  @redaction_statuses [:redacted, :redacted_with_omissions]
  @export_formats [:json, :ndjson, :zip]

  @required_binary_fields [
    :tenant_ref,
    :installation_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :export_ref,
    :incident_ref,
    :redaction_manifest_ref,
    :checksum,
    :created_by_operator_ref
  ]

  @optional_binary_fields [
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :download_ref,
    :expires_at
  ]

  @forbidden_raw_fields [
    :raw_prompt,
    :raw_provider_body,
    :raw_lower_payload,
    :raw_workflow_history,
    :raw_artifact,
    :tenant_secret,
    :provider_secret,
    :unredacted_payload
  ]

  @enforce_keys [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :export_ref,
    :incident_ref,
    :included_ref_set,
    :redaction_manifest_ref,
    :checksum,
    :created_by_operator_ref,
    :export_format,
    :redaction_status
  ]

  defstruct [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :export_ref,
    :incident_ref,
    :included_ref_set,
    :redaction_manifest_ref,
    :checksum,
    :created_by_operator_ref,
    :artifact_refs,
    :omitted_field_refs,
    :download_ref,
    :expires_at,
    :export_format,
    :redaction_status,
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          contract_name: String.t(),
          tenant_ref: String.t(),
          installation_ref: String.t(),
          workspace_ref: String.t() | nil,
          project_ref: String.t() | nil,
          environment_ref: String.t() | nil,
          principal_ref: String.t() | nil,
          system_actor_ref: String.t() | nil,
          resource_ref: String.t(),
          authority_packet_ref: String.t(),
          permission_decision_ref: String.t(),
          idempotency_key: String.t(),
          trace_id: String.t(),
          correlation_id: String.t(),
          release_manifest_ref: String.t(),
          export_ref: String.t(),
          incident_ref: String.t(),
          included_ref_set: [String.t()],
          redaction_manifest_ref: String.t(),
          checksum: String.t(),
          created_by_operator_ref: String.t(),
          artifact_refs: [String.t()],
          omitted_field_refs: [String.t()],
          download_ref: String.t() | nil,
          expires_at: String.t() | nil,
          export_format: atom(),
          redaction_status: atom(),
          metadata: map()
        }

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, {:forbidden_raw_fields, [atom()]}}
          | {:error, {:invalid_export_format, term()}}
          | {:error, {:invalid_redaction_status, term()}}
          | {:error, :invalid_incident_export_bundle}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    attrs = normalize_attrs(attrs)

    with :ok <- reject_forbidden_raw_fields(attrs),
         [] <- missing_required_fields(attrs),
         {:ok, export_format} <- normalize_enum(Map.get(attrs, :export_format), @export_formats),
         {:ok, redaction_status} <-
           normalize_enum(Map.get(attrs, :redaction_status), @redaction_statuses),
         true <- optional_binary_fields?(attrs),
         true <- string_list?(Map.get(attrs, :artifact_refs, [])),
         true <- string_list?(Map.get(attrs, :omitted_field_refs, [])),
         metadata <- Map.get(attrs, :metadata, %{}),
         true <- is_map(metadata) do
      {:ok, build(attrs, export_format, redaction_status, metadata)}
    else
      {:error, {:invalid_enum, value, allowed}} when allowed == @export_formats ->
        {:error, {:invalid_export_format, value}}

      {:error, {:invalid_enum, value, allowed}} when allowed == @redaction_statuses ->
        {:error, {:invalid_redaction_status, value}}

      fields when is_list(fields) ->
        {:error, {:missing_required_fields, fields}}

      {:error, _reason} = error ->
        error

      _error ->
        {:error, :invalid_incident_export_bundle}
    end
  end

  def new(_attrs), do: {:error, :invalid_incident_export_bundle}

  defp build(attrs, export_format, redaction_status, metadata) do
    %__MODULE__{
      contract_name: @contract_name,
      tenant_ref: Map.fetch!(attrs, :tenant_ref),
      installation_ref: Map.fetch!(attrs, :installation_ref),
      workspace_ref: Map.get(attrs, :workspace_ref),
      project_ref: Map.get(attrs, :project_ref),
      environment_ref: Map.get(attrs, :environment_ref),
      principal_ref: Map.get(attrs, :principal_ref),
      system_actor_ref: Map.get(attrs, :system_actor_ref),
      resource_ref: Map.fetch!(attrs, :resource_ref),
      authority_packet_ref: Map.fetch!(attrs, :authority_packet_ref),
      permission_decision_ref: Map.fetch!(attrs, :permission_decision_ref),
      idempotency_key: Map.fetch!(attrs, :idempotency_key),
      trace_id: Map.fetch!(attrs, :trace_id),
      correlation_id: Map.fetch!(attrs, :correlation_id),
      release_manifest_ref: Map.fetch!(attrs, :release_manifest_ref),
      export_ref: Map.fetch!(attrs, :export_ref),
      incident_ref: Map.fetch!(attrs, :incident_ref),
      included_ref_set: Map.fetch!(attrs, :included_ref_set),
      redaction_manifest_ref: Map.fetch!(attrs, :redaction_manifest_ref),
      checksum: Map.fetch!(attrs, :checksum),
      created_by_operator_ref: Map.fetch!(attrs, :created_by_operator_ref),
      artifact_refs: Map.get(attrs, :artifact_refs, []),
      omitted_field_refs: Map.get(attrs, :omitted_field_refs, []),
      download_ref: Map.get(attrs, :download_ref),
      expires_at: Map.get(attrs, :expires_at),
      export_format: export_format,
      redaction_status: redaction_status,
      metadata: metadata
    }
  end

  defp reject_forbidden_raw_fields(attrs) do
    present_forbidden =
      @forbidden_raw_fields
      |> Enum.filter(&Map.has_key?(attrs, &1))

    case present_forbidden do
      [] -> :ok
      fields -> {:error, {:forbidden_raw_fields, fields}}
    end
  end

  defp missing_required_fields(attrs) do
    binary_missing =
      Enum.reject(@required_binary_fields, fn field -> present_binary?(Map.get(attrs, field)) end)

    actor_missing =
      if present_binary?(Map.get(attrs, :principal_ref)) or
           present_binary?(Map.get(attrs, :system_actor_ref)) do
        []
      else
        [:principal_ref_or_system_actor_ref]
      end

    included_missing =
      if string_list?(Map.get(attrs, :included_ref_set)) and
           Map.get(attrs, :included_ref_set) != [] do
        []
      else
        [:included_ref_set]
      end

    enum_missing =
      [:export_format, :redaction_status]
      |> Enum.reject(&Map.has_key?(attrs, &1))

    binary_missing ++ actor_missing ++ included_missing ++ enum_missing
  end

  defp normalize_attrs(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {String.to_atom(key), value}
      {key, value} -> {key, value}
    end)
  end

  defp normalize_enum(value, allowed) when is_atom(value) do
    if value in allowed do
      {:ok, value}
    else
      {:error, {:invalid_enum, value, allowed}}
    end
  end

  defp normalize_enum(value, allowed) when is_binary(value) do
    value
    |> String.to_existing_atom()
    |> normalize_enum(allowed)
  rescue
    ArgumentError -> {:error, {:invalid_enum, value, allowed}}
  end

  defp normalize_enum(value, allowed), do: {:error, {:invalid_enum, value, allowed}}

  defp optional_binary_fields?(attrs) do
    Enum.all?(@optional_binary_fields, fn field ->
      value = Map.get(attrs, field)
      is_nil(value) or present_binary?(value)
    end)
  end

  defp string_list?([_ | _] = values), do: Enum.all?(values, &present_binary?/1)
  defp string_list?([]), do: true
  defp string_list?(_values), do: false

  defp present_binary?(value), do: is_binary(value) and byte_size(value) > 0
end
