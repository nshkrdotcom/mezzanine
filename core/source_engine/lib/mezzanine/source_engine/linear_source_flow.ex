defmodule Mezzanine.SourceEngine.LinearSourceFlow do
  @moduledoc """
  Linear source intake and publication shaping for governed source coordinators.

  This module is provider-output aware, but provider-effect free. Live reads and
  writes stay below Citadel/Jido; this module builds the canonical Linear
  operation inputs and normalizes their public-safe results into source
  admission attrs or publication receipts.
  """

  alias Mezzanine.SourceEngine.{LinearIssue, SourceBinding}

  @candidate_states ["submitted", "retry_submission"]
  @default_page_size 50

  @spec candidate_fetch_input(SourceBinding.t() | map(), keyword() | map()) ::
          {:ok, map()} | {:error, term()}
  def candidate_fetch_input(binding, opts \\ []) when is_map(binding) do
    opts = normalize_attrs(opts)
    filters = filters(binding, opts)

    with {:ok, assignee_id} <- assignee_filter(filters, opts) do
      filter =
        %{}
        |> maybe_put(:project_slug, string_value(filters, :project_slug))
        |> maybe_put(:state_names, state_names(binding, filters))
        |> maybe_put(:assignee_id, assignee_id)
        |> compact()

      {:ok,
       %{}
       |> maybe_put(:filter, filter)
       |> maybe_put(:first, positive_integer(value(opts, :first) || value(opts, :page_size)))
       |> maybe_put(:after, string_value(opts, :after) || string_value(opts, :cursor))
       |> compact()}
    end
  end

  @spec current_state_fetch_inputs([String.t()], SourceBinding.t() | map(), keyword() | map()) ::
          {:ok, [map()]} | {:error, term()}
  def current_state_fetch_inputs(issue_ids, _binding, opts \\ []) when is_list(issue_ids) do
    opts = normalize_attrs(opts)
    page_size = positive_integer(value(opts, :page_size) || value(opts, :first))

    issue_ids
    |> string_list()
    |> Enum.uniq()
    |> Enum.chunk_every(page_size)
    |> Enum.map(fn ids ->
      %{
        filter: %{issue_ids: ids},
        first: length(ids)
      }
    end)
    |> then(&{:ok, &1})
  end

  @spec refresh_issue_input(String.t() | map(), keyword() | map()) ::
          {:ok, map()} | {:error, term()}
  def refresh_issue_input(issue_or_attrs, opts \\ [])

  def refresh_issue_input(issue_id, _opts) when is_binary(issue_id) and issue_id != "" do
    {:ok, %{issue_id: issue_id}}
  end

  def refresh_issue_input(%{} = attrs, _opts) do
    case string_value(attrs, :issue_id) || string_value(attrs, :provider_external_ref) ||
           string_value(attrs, :id) do
      issue_id when is_binary(issue_id) and issue_id != "" -> {:ok, %{issue_id: issue_id}}
      _missing -> {:error, :missing_linear_issue_id}
    end
  end

  @spec issue_state_update_input(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def issue_state_update_input(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, issue_id} <- required_string(attrs, :issue_id),
         {:ok, state_id} <- required_string(attrs, :state_id) do
      {:ok, %{issue_id: issue_id, state_id: state_id}}
    end
  end

  @spec issue_state_lookup_input(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def issue_state_lookup_input(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, state_name} <- required_string(attrs, :state_name) do
      filter =
        %{
          state_names: [state_name]
        }
        |> maybe_put(:team_id, string_value(attrs, :team_id))

      {:ok, %{filter: filter, first: positive_integer(value(attrs, :state_lookup_first) || 10)}}
    end
  end

  @spec issue_state_id_from_lookup(map(), map() | keyword()) ::
          {:ok, String.t()} | {:error, term()}
  def issue_state_id_from_lookup(output, attrs) when is_map(output) do
    attrs = normalize_attrs(attrs)
    state_name = string_value(attrs, :state_name)
    team_id = string_value(attrs, :team_id)

    output
    |> value(:workflow_states)
    |> List.wrap()
    |> Enum.find(&workflow_state_match?(&1, state_name, team_id))
    |> case do
      %{} = state ->
        case string_value(state, :id) do
          state_id when is_binary(state_id) -> {:ok, state_id}
          _missing -> {:error, {:missing_linear_workflow_state_id, state_name}}
        end

      _missing ->
        {:error, {:missing_linear_workflow_state, state_name}}
    end
  end

  @spec publication_input(map() | keyword()) :: {:ok, {String.t(), map()}} | {:error, term()}
  def publication_input(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    with {:ok, body} <- required_string(attrs, :body) do
      comment_id = string_value(attrs, :comment_id) || string_value(attrs, :workpad_comment_id)
      publication_input_for(attrs, body, comment_id)
    end
  end

  @spec normalize_candidate_page(map(), map(), SourceBinding.t() | map()) ::
          {:ok, map()} | {:error, term()}
  def normalize_candidate_page(output, envelope, binding)
      when is_map(output) and is_map(envelope) and is_map(binding) do
    issues = output |> value(:issues) |> List.wrap() |> Enum.filter(&is_map/1)

    with {:ok, subject_attrs} <- normalize_issues(issues, envelope, binding) do
      {:ok,
       %{
         operation: "linear.issues.list",
         source_binding_id: source_binding_id(binding),
         issues: issues,
         subject_attrs: subject_attrs,
         page_info: value(output, :page_info) || %{},
         auth_binding: value(output, :auth_binding) || %{}
       }}
    end
  end

  @spec normalize_current_state_page(map(), map(), SourceBinding.t() | map(), [String.t()]) ::
          {:ok, map()} | {:error, term()}
  def normalize_current_state_page(output, envelope, binding, requested_issue_ids)
      when is_map(output) and is_map(envelope) and is_map(binding) and
             is_list(requested_issue_ids) do
    issues = output |> value(:issues) |> List.wrap() |> Enum.filter(&is_map/1)
    ordered_issues = order_issues_by_requested_ids(issues, requested_issue_ids)

    with {:ok, subject_attrs} <- normalize_issues(ordered_issues, envelope, binding) do
      {:ok,
       %{
         operation: "linear.issues.list",
         source_binding_id: source_binding_id(binding),
         issues: ordered_issues,
         subject_attrs: subject_attrs,
         missing_issue_ids: missing_issue_ids(issues, requested_issue_ids),
         page_info: value(output, :page_info) || %{},
         auth_binding: value(output, :auth_binding) || %{}
       }}
    end
  end

  @spec normalize_issue_refresh(map(), map(), SourceBinding.t() | map()) ::
          {:ok, map()} | {:error, term()}
  def normalize_issue_refresh(output, envelope, binding)
      when is_map(output) and is_map(envelope) and is_map(binding) do
    with %{} = issue <- value(output, :issue),
         {:ok, attrs} <- LinearIssue.subject_attrs(issue, envelope, binding) do
      {:ok,
       %{
         operation: "linear.issues.retrieve",
         source_binding_id: source_binding_id(binding),
         issue: issue,
         subject_attrs: attrs,
         auth_binding: value(output, :auth_binding) || %{}
       }}
    else
      nil -> {:error, :missing_linear_issue}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec publication_receipt(map(), map() | keyword()) :: {:ok, map()} | {:error, term()}
  def publication_receipt(dispatch_result, attrs)
      when is_map(dispatch_result) and (is_map(attrs) or is_list(attrs)) do
    attrs = normalize_attrs(attrs)

    with {:ok, source_publish_ref} <- required_string(attrs, :source_publish_ref),
         {:ok, source_binding_id} <- required_string(attrs, :source_binding_id),
         {:ok, source_ref} <- required_string(attrs, :source_ref) do
      envelope = Map.get(dispatch_result, :governed_lower_envelope)
      lower_receipt = Map.get(dispatch_result, :governed_lower_receipt)
      output = Map.get(dispatch_result, :output, %{})
      comment = value(output, :comment) || %{}

      {:ok,
       %{
         source_publication_receipt_ref:
           "source-publication://#{source_binding_id}/#{digest([source_publish_ref, source_ref])}",
         source_publish_ref: source_publish_ref,
         source_binding_id: source_binding_id,
         source_ref: source_ref,
         status: publication_status(output),
         capability_id: capability_id(envelope, attrs),
         lower_runtime_kind: lower_runtime_kind(envelope),
         lower_request_ref: field_value(envelope, :lower_request_ref),
         lower_receipt_ref: field_value(lower_receipt, :lower_receipt_ref),
         authority_ref: field_value(envelope, :authority_ref),
         authority_decision_hash: field_value(envelope, :authority_decision_hash),
         connector_manifest_ref: field_value(envelope, :connector_manifest_ref),
         connector_manifest_hash: field_value(envelope, :connector_manifest_hash),
         capability_negotiation_ref: field_value(envelope, :capability_negotiation_ref),
         provider_response_ref: provider_response_ref(dispatch_result, lower_receipt),
         redaction_manifest_ref:
           string_value(attrs, :redaction_manifest_ref) ||
             field_value(envelope, :redaction_profile_ref),
         workpad_refs: workpad_refs(attrs, comment),
         comment_ref: comment_ref(comment),
         comment_id: comment_id(attrs, comment),
         issue_id: string_value(attrs, :issue_id),
         output_ref: string_value(attrs, :output_ref),
         trace_id: string_value(attrs, :trace_id) || field_value(envelope, :trace_id)
       }
       |> compact()}
    end
  end

  @spec issue_state_update_receipt(map(), map() | keyword()) :: {:ok, map()} | {:error, term()}
  def issue_state_update_receipt(dispatch_result, attrs)
      when is_map(dispatch_result) and (is_map(attrs) or is_list(attrs)) do
    attrs = normalize_attrs(attrs)

    with {:ok, source_publish_ref} <- required_string(attrs, :source_publish_ref),
         {:ok, source_binding_id} <- required_string(attrs, :source_binding_id),
         {:ok, source_ref} <- required_string(attrs, :source_ref),
         {:ok, issue_id} <- required_string(attrs, :issue_id),
         {:ok, state_id} <- required_string(attrs, :state_id) do
      envelope = Map.get(dispatch_result, :governed_lower_envelope)
      lower_receipt = Map.get(dispatch_result, :governed_lower_receipt)
      output = Map.get(dispatch_result, :output, %{})
      issue = value(output, :issue) || %{}

      receipt =
        %{
          source_publication_receipt_ref:
            "source-publication://#{source_binding_id}/#{digest([source_publish_ref, source_ref, state_id])}",
          source_publish_ref: source_publish_ref,
          source_binding_id: source_binding_id,
          source_ref: source_ref,
          status: publication_status(output),
          capability_id:
            capability_id(envelope, Map.put(attrs, :capability_id, "linear.issues.update")),
          lower_runtime_kind: lower_runtime_kind(envelope),
          lower_request_ref: field_value(envelope, :lower_request_ref),
          lower_receipt_ref: field_value(lower_receipt, :lower_receipt_ref),
          authority_ref: field_value(envelope, :authority_ref),
          authority_decision_hash: field_value(envelope, :authority_decision_hash),
          connector_manifest_ref: field_value(envelope, :connector_manifest_ref),
          connector_manifest_hash: field_value(envelope, :connector_manifest_hash),
          capability_negotiation_ref: field_value(envelope, :capability_negotiation_ref),
          provider_response_ref: provider_response_ref(dispatch_result, lower_receipt),
          redaction_manifest_ref:
            string_value(attrs, :redaction_manifest_ref) ||
              field_value(envelope, :redaction_profile_ref),
          workpad_refs: [],
          issue_id: issue_id,
          issue_identifier: string_value(issue, :identifier),
          state_id: state_id,
          state_name: string_value(attrs, :state_name),
          state_lookup_lower_request_ref: string_value(attrs, :state_lookup_lower_request_ref),
          state_lookup_lower_receipt_ref: string_value(attrs, :state_lookup_lower_receipt_ref),
          output_ref: string_value(attrs, :output_ref),
          trace_id: string_value(attrs, :trace_id) || field_value(envelope, :trace_id)
        }
        |> compact()
        |> Map.put(:workpad_refs, [])

      {:ok, receipt}
    end
  end

  defp normalize_issues(issues, envelope, binding) do
    Enum.reduce_while(issues, {:ok, []}, fn issue, {:ok, acc} ->
      case LinearIssue.subject_attrs(issue, envelope, binding) do
        {:ok, attrs} -> {:cont, {:ok, [attrs | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, attrs} -> {:ok, Enum.reverse(attrs)}
      error -> error
    end
  end

  defp order_issues_by_requested_ids(issues, requested_issue_ids) do
    by_id =
      Map.new(issues, fn issue ->
        {string_value(issue, :id), issue}
      end)

    requested_issue_ids
    |> string_list()
    |> Enum.uniq()
    |> Enum.flat_map(fn issue_id ->
      case Map.get(by_id, issue_id) do
        nil -> []
        issue -> [issue]
      end
    end)
  end

  defp missing_issue_ids(issues, requested_issue_ids) do
    present_ids =
      issues
      |> Enum.map(&string_value(&1, :id))
      |> MapSet.new()

    requested_issue_ids
    |> string_list()
    |> Enum.uniq()
    |> Enum.reject(&MapSet.member?(present_ids, &1))
  end

  defp filters(binding, opts) do
    binding_filters =
      case value(binding, :candidate_filters) do
        %{} = filters -> filters
        _other -> %{}
      end

    Map.merge(binding_filters, Map.get(opts, :filter, %{}) || %{})
  end

  defp assignee_filter(filters, opts) do
    case string_value(filters, :assignee_id) || string_value(filters, :assignee) do
      nil ->
        {:ok, nil}

      "me" ->
        viewer_assignee_filter(value(opts, :viewer))

      assignee_id ->
        {:ok, assignee_id}
    end
  end

  defp state_names(binding, %{} = filters) do
    case value(filters, :state_names) do
      values when is_list(values) -> string_list(values)
      value when is_binary(value) -> [value]
      _other -> mapped_candidate_states(binding)
    end
  end

  defp mapped_candidate_states(binding) do
    binding
    |> value(:state_mapping)
    |> candidate_state_names()
    |> nil_if_empty()
  end

  defp publication_input_for(_attrs, body, comment_id) when is_binary(comment_id) do
    {:ok, {"linear.comments.update", %{comment_id: comment_id, body: body}}}
  end

  defp publication_input_for(attrs, body, _comment_id) do
    if fallback_allowed?(attrs),
      do: create_publication_input(attrs, body),
      else: {:error, :missing_linear_comment_id}
  end

  defp create_publication_input(attrs, body) do
    with {:ok, issue_id} <- required_string(attrs, :issue_id) do
      {:ok, {"linear.comments.create", %{issue_id: issue_id, body: body}}}
    end
  end

  defp viewer_assignee_filter(%{} = viewer) do
    viewer
    |> string_value(:id)
    |> present_assignee_id()
  end

  defp viewer_assignee_filter(_viewer), do: {:error, :linear_viewer_required_for_me_assignee}

  defp present_assignee_id(nil), do: {:error, :missing_linear_viewer_id}
  defp present_assignee_id(viewer_id), do: {:ok, viewer_id}

  defp candidate_state_names(%{} = mapping) do
    mapping
    |> Enum.flat_map(&candidate_state_values/1)
    |> Enum.uniq()
  end

  defp candidate_state_names(_mapping), do: []

  defp candidate_state_values({canonical, provider_states}) do
    if to_string(canonical) in @candidate_states,
      do: string_list(List.wrap(provider_states)),
      else: []
  end

  defp nil_if_empty([]), do: nil
  defp nil_if_empty(values), do: values

  defp source_binding_id(binding),
    do: string_value(binding, :source_binding_id) || "linear_primary"

  defp fallback_allowed?(attrs) do
    value(attrs, :allow_create_fallback?) == true or
      value(attrs, :create_fallback?) == true or
      value(attrs, :fallback) in [:create, "create"]
  end

  defp publication_status(%{} = output) do
    case value(output, :success) do
      false -> "failed"
      _other -> "published"
    end
  end

  defp capability_id(envelope, attrs) do
    field_value(envelope, :capability_id) || string_value(attrs, :capability_id)
  end

  defp lower_runtime_kind(envelope) do
    case field_value(envelope, :lower_runtime_kind) do
      value when is_atom(value) -> Atom.to_string(value)
      value -> value
    end
  end

  defp provider_response_ref(dispatch_result, lower_receipt) do
    dispatch_result
    |> Map.get(:artifact_refs, [])
    |> List.wrap()
    |> List.first()
    |> case do
      ref when is_binary(ref) -> ref
      _other -> field_value(lower_receipt, :lower_receipt_ref)
    end
  end

  defp workpad_refs(attrs, comment) do
    attrs
    |> value(:workpad_refs)
    |> List.wrap()
    |> string_list()
    |> case do
      [] ->
        case comment_ref(comment) do
          nil -> []
          ref -> [ref]
        end

      refs ->
        refs
    end
  end

  defp workflow_state_match?(%{} = state, state_name, team_id) do
    workflow_state_name_match?(string_value(state, :name), state_name) and
      workflow_state_team_match?(state, team_id)
  end

  defp workflow_state_match?(_state, _state_name, _team_id), do: false

  defp workflow_state_name_match?(name, state_name)
       when is_binary(name) and is_binary(state_name) do
    String.downcase(name) == String.downcase(state_name)
  end

  defp workflow_state_name_match?(_name, _state_name), do: false

  defp workflow_state_team_match?(_state, nil), do: true

  defp workflow_state_team_match?(%{} = state, team_id) when is_binary(team_id) do
    state
    |> value(:team)
    |> case do
      %{} = team -> string_value(team, :id) == team_id
      _missing -> false
    end
  end

  defp comment_ref(%{} = comment) do
    case string_value(comment, :id) do
      nil -> nil
      id -> "linear-comment://#{id}"
    end
  end

  defp comment_id(attrs, comment) do
    string_value(comment, :id) || string_value(attrs, :comment_id)
  end

  defp field_value(nil, _key), do: nil

  defp field_value(%_{} = struct, key), do: Map.get(struct, key)
  defp field_value(%{} = map, key), do: value(map, key)

  defp positive_integer(nil), do: @default_page_size
  defp positive_integer(value) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value), do: @default_page_size

  defp required_string(attrs, key) do
    case string_value(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_required, key}}
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, _key, []), do: map
  defp maybe_put(map, _key, value) when value == %{}, do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp compact(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", [], %{}] end)
    |> Map.new()
  end

  defp value(%_{} = struct, key), do: struct |> Map.from_struct() |> value(key)
  defp value(%{} = map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp value(_map, _key), do: nil

  defp string_value(map, key) do
    case value(map, key) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: nil, else: value

      value when is_atom(value) and not is_nil(value) ->
        Atom.to_string(value)

      _other ->
        nil
    end
  end

  defp string_list(values) when is_list(values) do
    values
    |> Enum.map(fn
      value when is_binary(value) -> String.trim(value)
      value when is_atom(value) -> Atom.to_string(value)
      _other -> nil
    end)
    |> Enum.reject(&(&1 in [nil, ""]))
  end

  defp string_list(_values), do: []

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(%{} = attrs), do: Map.new(attrs)

  defp digest(value) do
    value
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, 24)
  end
end
