defmodule Mezzanine.ControlRoom.IncidentBundle do
  @moduledoc """
  Release-linked incident bundle contract for operator control-room evidence.

  The bundle is intentionally a compact reference envelope. It carries tenant,
  authority, trace, workflow, lower-fact, semantic, projection, and release
  references so an operator can reconstruct an incident without embedding raw
  lower payloads, workflow history, provider metadata, prompts, or artifacts.
  """

  @contract_name "Mezzanine.IncidentBundle.v1"
  @staleness_classes [
    :queued,
    :in_flight,
    :delivered_to_temporal,
    :pending_workflow_ack,
    :processed,
    :dispatch_failed,
    :stale,
    :lower_fresh,
    :projection_stale,
    :diagnostic_only,
    :authoritative_archived
  ]
  @required_binary_fields [
    :tenant_ref,
    :installation_ref,
    :principal_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :incident_ref,
    :command_ref,
    :workflow_ref,
    :signal_ref,
    :activity_ref,
    :semantic_ref,
    :projection_ref
  ]
  @optional_binary_fields [:workspace_ref, :project_ref, :environment_ref, :system_actor_ref]
  @normalizable_fields @required_binary_fields ++
                         @optional_binary_fields ++
                         [:lower_fact_refs, :staleness_class, :evidence_refs, :metadata]
  @field_lookup Map.new(@normalizable_fields, &{Atom.to_string(&1), &1})
  @staleness_lookup Map.new(@staleness_classes, &{Atom.to_string(&1), &1})

  @enforce_keys [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :principal_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :incident_ref,
    :command_ref,
    :workflow_ref,
    :signal_ref,
    :activity_ref,
    :lower_fact_refs,
    :semantic_ref,
    :projection_ref,
    :staleness_class
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
    :command_ref,
    :workflow_ref,
    :signal_ref,
    :activity_ref,
    :lower_fact_refs,
    :semantic_ref,
    :projection_ref,
    :staleness_class,
    evidence_refs: [],
    metadata: %{}
  ]

  @type t :: %__MODULE__{
          contract_name: String.t(),
          tenant_ref: String.t(),
          installation_ref: String.t(),
          workspace_ref: String.t() | nil,
          project_ref: String.t() | nil,
          environment_ref: String.t() | nil,
          principal_ref: String.t(),
          system_actor_ref: String.t() | nil,
          resource_ref: String.t(),
          authority_packet_ref: String.t(),
          permission_decision_ref: String.t(),
          idempotency_key: String.t(),
          trace_id: String.t(),
          correlation_id: String.t(),
          release_manifest_ref: String.t(),
          incident_ref: String.t(),
          command_ref: String.t(),
          workflow_ref: String.t(),
          signal_ref: String.t(),
          activity_ref: String.t(),
          lower_fact_refs: [String.t()],
          semantic_ref: String.t(),
          projection_ref: String.t(),
          staleness_class: atom(),
          evidence_refs: [String.t()],
          metadata: map()
        }

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, {:invalid_staleness_class, term()}}
          | {:error, :invalid_incident_bundle}
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    attrs = normalize_attrs(attrs)

    case missing_required_fields(attrs) do
      [] ->
        build(attrs)

      fields ->
        {:error, {:missing_required_fields, fields}}
    end
  end

  def new(_attrs), do: {:error, :invalid_incident_bundle}

  defp build(attrs) do
    with {:ok, staleness_class} <- normalize_staleness(Map.get(attrs, :staleness_class)),
         true <- optional_binary_fields?(attrs),
         true <- string_list?(Map.get(attrs, :evidence_refs, [])),
         metadata <- Map.get(attrs, :metadata, %{}),
         true <- is_map(metadata) do
      {:ok,
       %__MODULE__{
         contract_name: @contract_name,
         tenant_ref: Map.fetch!(attrs, :tenant_ref),
         installation_ref: Map.fetch!(attrs, :installation_ref),
         workspace_ref: Map.get(attrs, :workspace_ref),
         project_ref: Map.get(attrs, :project_ref),
         environment_ref: Map.get(attrs, :environment_ref),
         principal_ref: Map.fetch!(attrs, :principal_ref),
         system_actor_ref: Map.get(attrs, :system_actor_ref),
         resource_ref: Map.fetch!(attrs, :resource_ref),
         authority_packet_ref: Map.fetch!(attrs, :authority_packet_ref),
         permission_decision_ref: Map.fetch!(attrs, :permission_decision_ref),
         idempotency_key: Map.fetch!(attrs, :idempotency_key),
         trace_id: Map.fetch!(attrs, :trace_id),
         correlation_id: Map.fetch!(attrs, :correlation_id),
         release_manifest_ref: Map.fetch!(attrs, :release_manifest_ref),
         incident_ref: Map.fetch!(attrs, :incident_ref),
         command_ref: Map.fetch!(attrs, :command_ref),
         workflow_ref: Map.fetch!(attrs, :workflow_ref),
         signal_ref: Map.fetch!(attrs, :signal_ref),
         activity_ref: Map.fetch!(attrs, :activity_ref),
         lower_fact_refs: Map.fetch!(attrs, :lower_fact_refs),
         semantic_ref: Map.fetch!(attrs, :semantic_ref),
         projection_ref: Map.fetch!(attrs, :projection_ref),
         staleness_class: staleness_class,
         evidence_refs: Map.get(attrs, :evidence_refs, []),
         metadata: metadata
       }}
    else
      {:error, {:invalid_staleness_class, _value}} = error -> error
      _ -> {:error, :invalid_incident_bundle}
    end
  end

  defp normalize_attrs(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {Map.get(@field_lookup, key, key), value}
      {key, value} -> {key, value}
    end)
  end

  defp missing_required_fields(attrs) do
    binary_missing =
      Enum.reject(@required_binary_fields, fn field -> present_binary?(Map.get(attrs, field)) end)

    list_missing =
      if string_list?(Map.get(attrs, :lower_fact_refs)) and Map.get(attrs, :lower_fact_refs) != [] do
        []
      else
        [:lower_fact_refs]
      end

    staleness_missing =
      if Map.has_key?(attrs, :staleness_class), do: [], else: [:staleness_class]

    binary_missing ++ list_missing ++ staleness_missing
  end

  defp optional_binary_fields?(attrs) do
    Enum.all?(@optional_binary_fields, fn field ->
      value = Map.get(attrs, field)
      is_nil(value) or present_binary?(value)
    end)
  end

  defp normalize_staleness(value) when is_atom(value) and value in @staleness_classes,
    do: {:ok, value}

  defp normalize_staleness(value) when is_binary(value) do
    case Map.fetch(@staleness_lookup, value) do
      {:ok, staleness} -> {:ok, staleness}
      :error -> {:error, {:invalid_staleness_class, value}}
    end
  end

  defp normalize_staleness(value), do: {:error, {:invalid_staleness_class, value}}

  defp string_list?([_ | _] = values), do: Enum.all?(values, &present_binary?/1)
  defp string_list?([]), do: true
  defp string_list?(_values), do: false

  defp present_binary?(value), do: is_binary(value) and byte_size(value) > 0
end
