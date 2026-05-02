defmodule Mezzanine.SourceEngine.LinearIssue do
  @moduledoc """
  Converts normalized Jido Linear issue output into subject ledger attrs.

  Source refs produced here are provenance facts. They are not authority,
  credential, lease, or tenant-authorization evidence.
  """

  alias Mezzanine.SourceEngine.{Admission, SourceBinding}

  @provider "linear"
  @subject_kind "linear_coding_ticket"
  @schema_ref "mezzanine.subject.linear_coding_ticket.payload.v1"
  @schema_version 1
  @payload_schema_revision "linear.issue.v1"

  @ledger_forbidden_keys [:tenant_id, :tenant_scope, :authorization_scope, :authority_ref]

  @type issue :: map()
  @type envelope :: map()
  @type binding :: SourceBinding.t() | map()

  @spec subject_attrs(issue(), envelope(), binding()) :: {:ok, map()} | {:error, term()}
  def subject_attrs(issue, envelope, binding \\ %{})

  def subject_attrs(issue, envelope, binding)
      when is_map(issue) and is_map(envelope) and is_map(binding) do
    with {:ok, scope} <- normalize_scope(envelope, binding),
         {:ok, normalized_issue} <- normalize_issue(issue),
         {:ok, classification} <- classify(normalized_issue, binding) do
      attrs = build_subject_attrs(normalized_issue, scope, classification)

      {:ok, Map.drop(attrs, @ledger_forbidden_keys)}
    end
  end

  def subject_attrs(_issue, _envelope, _binding), do: {:error, :invalid_linear_issue_input}

  @spec ledger_forbidden_keys() :: [atom()]
  def ledger_forbidden_keys, do: @ledger_forbidden_keys

  defp normalize_scope(envelope, binding) do
    with {:ok, tenant_id} <- required_string(envelope, :tenant_id, :missing_tenant_scope),
         {:ok, installation_id} <-
           required_string(envelope, :installation_id, :missing_installation_scope),
         {:ok, source_binding_id} <- source_binding_id(envelope, binding),
         :ok <- validate_authorization_scope(envelope, tenant_id) do
      {:ok,
       %{
         tenant_id: tenant_id,
         installation_id: installation_id,
         source_binding_id: source_binding_id,
         trace_id: string_value(envelope, :trace_id),
         causation_id: string_value(envelope, :causation_id),
         actor_ref: actor_ref(envelope, tenant_id),
         state_mapping: map_value(envelope, :state_mapping) || state_mapping(binding)
       }}
    end
  end

  defp source_binding_id(envelope, binding) do
    value =
      string_value(envelope, :source_binding_id) ||
        string_value(envelope, :source_binding_ref) ||
        string_value(binding, :source_binding_id) ||
        string_value(binding, :source_binding_ref)

    if present?(value), do: {:ok, value}, else: {:error, :missing_source_binding}
  end

  defp validate_authorization_scope(envelope, tenant_id) do
    case map_value(envelope, :authorization_scope) do
      nil ->
        {:error, :authorization_scope_missing}

      authorization_scope ->
        if string_value(authorization_scope, :tenant_id) == tenant_id do
          :ok
        else
          {:error, :authorization_scope_mismatch}
        end
    end
  end

  defp normalize_issue(issue) do
    with {:ok, id} <- required_string(issue, :id, :missing_linear_issue_id),
         {:ok, identifier} <- required_string(issue, :identifier, :missing_linear_identifier),
         {:ok, title} <- required_string(issue, :title, :missing_linear_title) do
      {:ok,
       %{
         id: id,
         identifier: identifier,
         title: title,
         description: string_value(issue, :description),
         priority: integer_value(issue, :priority),
         labels: labels(issue),
         blockers: blockers(issue),
         branch_ref: string_value(issue, :branch_name) || string_value(issue, :branch_ref),
         source_url: string_value(issue, :url) || string_value(issue, :source_url),
         source_state: state_name(value(issue, :state)),
         source_state_type: string_value(value(issue, :state), :type),
         assignee: compact_user(value(issue, :assignee)),
         project: compact_project(value(issue, :project)),
         team: compact_team(value(issue, :team)),
         created_at: string_value(issue, :created_at),
         updated_at: string_value(issue, :updated_at)
       }}
    end
  end

  defp classify(issue, binding) do
    payload =
      issue
      |> Map.take([:id, :identifier, :title, :priority, :labels, :blockers])
      |> Map.put(:state, %{name: issue.source_state, type: issue.source_state_type})
      |> Map.put(:assigned_to_worker, true)

    {classification, decision} =
      Admission.classify_candidate(payload, binding_with_defaults(binding))

    {:ok, %{classification: classification, decision: decision}}
  end

  defp binding_with_defaults(%SourceBinding{} = binding), do: binding

  defp binding_with_defaults(binding) when is_map(binding) do
    state_mapping =
      case state_mapping(binding) do
        mapping when map_size(mapping) > 0 ->
          mapping

        _empty ->
          %{
            "submitted" => ["Todo"],
            "completed" => ["Done", "Completed"],
            "rejected" => ["Canceled", "Cancelled", "Duplicate"]
          }
      end

    %SourceBinding{
      source_binding_id: string_value(binding, :source_binding_id) || "linear_primary",
      installation_id: string_value(binding, :installation_id) || "unknown-installation",
      provider: @provider,
      connection_ref: string_value(binding, :connection_ref) || "linear",
      state_mapping: state_mapping
    }
  end

  defp build_subject_attrs(issue, scope, classification) do
    source_ref = source_ref(scope.installation_id, issue.identifier)
    provider_revision = issue.updated_at || issue.created_at || issue.id
    source_event_id = source_event_id(scope, issue, provider_revision)
    lifecycle_state = lifecycle_state(classification)

    %{
      installation_id: scope.installation_id,
      source_ref: source_ref,
      source_event_id: source_event_id,
      source_binding_id: scope.source_binding_id,
      provider: @provider,
      provider_external_ref: issue.id,
      provider_revision: provider_revision,
      source_state: issue.source_state,
      state_mapping: state_mapping_attr(issue, classification),
      blocker_refs: issue.blockers,
      labels: issue.labels,
      priority: issue.priority,
      branch_ref: issue.branch_ref,
      source_url: issue.source_url,
      source_routing: source_routing(issue, source_ref),
      payload_schema_revision: @payload_schema_revision,
      subject_kind: @subject_kind,
      lifecycle_state: lifecycle_state,
      title: issue.title,
      description: issue.description,
      schema_ref: @schema_ref,
      schema_version: @schema_version,
      payload: payload(issue),
      opened_at: parse_datetime(issue.created_at),
      trace_id: scope.trace_id || "trace-#{source_event_id}",
      causation_id: scope.causation_id || source_event_id,
      actor_ref: scope.actor_ref
    }
  end

  defp lifecycle_state(%{decision: %{lifecycle_state: lifecycle_state}})
       when is_binary(lifecycle_state),
       do: lifecycle_state

  defp state_mapping_attr(issue, classification) do
    decision = classification.decision

    %{
      provider_state: issue.source_state,
      provider_state_type: issue.source_state_type,
      lifecycle_state: decision.lifecycle_state,
      canonical_state: decision.canonical_state,
      admission_classification: Atom.to_string(classification.classification),
      reason: Atom.to_string(decision.reason)
    }
  end

  defp source_routing(issue, source_ref) do
    %{
      assignee: issue.assignee,
      project: issue.project,
      team: issue.team,
      provenance: %{
        "provider" => @provider,
        "source_ref" => source_ref,
        "provider_external_ref" => issue.id
      }
    }
    |> Enum.reject(fn {_key, value} -> value in [nil, %{}] end)
    |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)
  end

  defp payload(issue) do
    %{
      "identifier" => issue.identifier,
      "source_kind" => @provider,
      "title" => issue.title
    }
  end

  defp labels(issue) do
    issue
    |> value(:labels)
    |> List.wrap()
    |> Enum.map(&trimmed_string/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&String.downcase/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp blockers(issue) do
    issue
    |> value(:blockers)
    |> List.wrap()
    |> Enum.map(&blocker_ref/1)
    |> Enum.reject(&is_nil/1)
  end

  defp blocker_ref(%{} = blocker) do
    related_issue = map_value(blocker, :issue) || blocker
    identifier = string_value(related_issue, :identifier)

    %{
      "provider" => @provider,
      "relation_id" => string_value(blocker, :id),
      "relation_type" => string_value(blocker, :type),
      "direction" => string_value(blocker, :direction),
      "provider_external_ref" => string_value(related_issue, :id),
      "identifier" => identifier,
      "source_ref" => maybe_related_source_ref(identifier),
      "source_state" => state_name(value(related_issue, :state)),
      "title" => string_value(related_issue, :title),
      "url" => string_value(related_issue, :url)
    }
    |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
    |> Map.new()
  end

  defp blocker_ref(_blocker), do: nil

  defp maybe_related_source_ref(nil), do: nil
  defp maybe_related_source_ref(identifier), do: "linear://issue/#{identifier}"

  defp source_ref(installation_id, identifier),
    do: "linear://#{installation_id}/issue/#{identifier}"

  defp source_event_id(scope, issue, provider_revision) do
    encoded =
      [
        scope.installation_id,
        scope.source_binding_id,
        issue.id,
        issue.identifier,
        provider_revision
      ]
      |> Enum.join("\n")

    "src_linear_" <> digest(encoded, 24)
  end

  defp compact_user(nil), do: nil

  defp compact_user(user) when is_map(user) do
    compact_string_map(user, [:id, :name, :email])
  end

  defp compact_project(nil), do: nil

  defp compact_project(project) when is_map(project) do
    compact_string_map(project, [:id, :name, :slug_id, :url])
  end

  defp compact_team(nil), do: nil

  defp compact_team(team) when is_map(team) do
    compact_string_map(team, [:id, :key, :name])
  end

  defp compact_string_map(map, keys) do
    keys
    |> Enum.flat_map(fn key ->
      case string_value(map, key) do
        nil -> []
        value -> [{Atom.to_string(key), value}]
      end
    end)
    |> Map.new()
  end

  defp state_name(nil), do: nil
  defp state_name(state) when is_binary(state), do: state
  defp state_name(state) when is_map(state), do: string_value(state, :name)
  defp state_name(_state), do: nil

  defp state_mapping(%SourceBinding{state_mapping: mapping}), do: mapping || %{}
  defp state_mapping(binding) when is_map(binding), do: map_value(binding, :state_mapping) || %{}

  defp actor_ref(envelope, tenant_id) do
    case map_value(envelope, :actor_ref) do
      nil -> %{"kind" => "system", "id" => "source-ingest", "tenant_id" => tenant_id}
      actor_ref -> actor_ref
    end
  end

  defp required_string(attrs, key, error) do
    case string_value(attrs, key) do
      value when is_binary(value) -> {:ok, value}
      nil -> {:error, error}
    end
  end

  defp integer_value(attrs, key) do
    case value(attrs, key) do
      value when is_integer(value) -> value
      _other -> nil
    end
  end

  defp parse_datetime(nil), do: nil

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      {:error, _reason} -> nil
    end
  end

  defp string_value(attrs, key), do: attrs |> value(key) |> trimmed_string()
  defp map_value(attrs, key), do: if(is_map(value(attrs, key)), do: value(attrs, key), else: nil)

  defp value(attrs, key) when is_map(attrs),
    do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp value(_attrs, _key), do: nil

  defp trimmed_string(nil), do: nil

  defp trimmed_string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp trimmed_string(value) when is_atom(value),
    do: value |> Atom.to_string() |> trimmed_string()

  defp trimmed_string(_value), do: nil

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(_value), do: false

  defp digest(value, length) do
    value
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, length)
  end
end
