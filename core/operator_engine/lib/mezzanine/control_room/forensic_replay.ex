defmodule Mezzanine.ControlRoom.ForensicReplay do
  @moduledoc """
  Release-linked forensic replay contract for incident-bundle evidence.

  The replay shape is intentionally compact and deterministic. It carries only
  ordered refs, integrity evidence, and scope metadata needed to replay an
  incident timeline. Raw workflow history, lower payloads, provider metadata,
  prompts, artifacts, and tenant-sensitive secrets are rejected at the boundary.
  """

  @contract_name "Mezzanine.ForensicReplay.v1"

  @required_binary_fields [
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :incident_ref,
    :timeline_ref,
    :integrity_hash,
    :replay_result_ref
  ]

  @actor_fields [:principal_ref, :system_actor_ref]

  @forbidden_raw_fields [
    :raw_prompt,
    :raw_provider_body,
    :raw_lower_payload,
    :raw_workflow_history,
    :raw_artifact,
    :raw_event_body,
    :tenant_secret,
    :provider_secret,
    :unredacted_payload
  ]
  @normalizable_fields @required_binary_fields ++
                         @actor_fields ++
                         @forbidden_raw_fields ++
                         [:ordered_event_refs, :missing_ref_set, :evidence_refs, :metadata]
  @field_lookup Map.new(@normalizable_fields, &{Atom.to_string(&1), &1})

  @enforce_keys [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :incident_ref,
    :timeline_ref,
    :ordered_event_refs,
    :integrity_hash,
    :missing_ref_set,
    :replay_result_ref
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
    :incident_ref,
    :timeline_ref,
    :ordered_event_refs,
    :integrity_hash,
    :missing_ref_set,
    :replay_result_ref,
    evidence_refs: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          contract_name: String.t(),
          tenant_ref: String.t(),
          installation_ref: String.t(),
          workspace_ref: String.t(),
          project_ref: String.t(),
          environment_ref: String.t(),
          principal_ref: String.t() | nil,
          system_actor_ref: String.t() | nil,
          resource_ref: String.t(),
          authority_packet_ref: String.t(),
          permission_decision_ref: String.t(),
          idempotency_key: String.t(),
          trace_id: String.t(),
          correlation_id: String.t(),
          release_manifest_ref: String.t(),
          incident_ref: String.t(),
          timeline_ref: String.t(),
          ordered_event_refs: [String.t()],
          integrity_hash: String.t(),
          missing_ref_set: [String.t()],
          replay_result_ref: String.t(),
          evidence_refs: [String.t()],
          metadata: map()
        }

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, {:forbidden_raw_fields, [atom()]}}
          | {:error, :invalid_forensic_replay}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    attrs = normalize_attrs(attrs)

    with :ok <- reject_forbidden_raw_fields(attrs),
         [] <- missing_required_fields(attrs),
         true <- string_list?(Map.get(attrs, :evidence_refs, [])),
         metadata <- Map.get(attrs, :metadata, %{}),
         true <- is_map(metadata) do
      {:ok, build(attrs, metadata)}
    else
      fields when is_list(fields) ->
        {:error, {:missing_required_fields, fields}}

      {:error, _reason} = error ->
        error

      _error ->
        {:error, :invalid_forensic_replay}
    end
  end

  def new(_attrs), do: {:error, :invalid_forensic_replay}

  defp build(attrs, metadata) do
    %__MODULE__{
      contract_name: @contract_name,
      tenant_ref: Map.fetch!(attrs, :tenant_ref),
      installation_ref: Map.fetch!(attrs, :installation_ref),
      workspace_ref: Map.fetch!(attrs, :workspace_ref),
      project_ref: Map.fetch!(attrs, :project_ref),
      environment_ref: Map.fetch!(attrs, :environment_ref),
      principal_ref: Map.get(attrs, :principal_ref),
      system_actor_ref: Map.get(attrs, :system_actor_ref),
      resource_ref: Map.fetch!(attrs, :resource_ref),
      authority_packet_ref: Map.fetch!(attrs, :authority_packet_ref),
      permission_decision_ref: Map.fetch!(attrs, :permission_decision_ref),
      idempotency_key: Map.fetch!(attrs, :idempotency_key),
      trace_id: Map.fetch!(attrs, :trace_id),
      correlation_id: Map.fetch!(attrs, :correlation_id),
      release_manifest_ref: Map.fetch!(attrs, :release_manifest_ref),
      incident_ref: Map.fetch!(attrs, :incident_ref),
      timeline_ref: Map.fetch!(attrs, :timeline_ref),
      ordered_event_refs: Map.fetch!(attrs, :ordered_event_refs),
      integrity_hash: Map.fetch!(attrs, :integrity_hash),
      missing_ref_set: Map.fetch!(attrs, :missing_ref_set),
      replay_result_ref: Map.fetch!(attrs, :replay_result_ref),
      evidence_refs: Map.get(attrs, :evidence_refs, []),
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
      if Enum.any?(@actor_fields, fn field -> present_binary?(Map.get(attrs, field)) end) do
        []
      else
        [:principal_ref_or_system_actor_ref]
      end

    ordered_missing =
      if string_list?(Map.get(attrs, :ordered_event_refs)) and
           Map.get(attrs, :ordered_event_refs) != [] do
        []
      else
        [:ordered_event_refs]
      end

    missing_ref_set_missing =
      if Map.has_key?(attrs, :missing_ref_set) and string_list?(Map.get(attrs, :missing_ref_set)) do
        []
      else
        [:missing_ref_set]
      end

    binary_missing ++ actor_missing ++ ordered_missing ++ missing_ref_set_missing
  end

  defp normalize_attrs(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {Map.get(@field_lookup, key, key), value}
      {key, value} -> {key, value}
    end)
  end

  defp string_list?([_ | _] = values), do: Enum.all?(values, &present_binary?/1)
  defp string_list?([]), do: true
  defp string_list?(_values), do: false

  defp present_binary?(value), do: is_binary(value) and byte_size(value) > 0
end
