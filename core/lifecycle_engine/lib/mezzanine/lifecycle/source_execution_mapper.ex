defmodule Mezzanine.Lifecycle.SourceExecutionMapper do
  @moduledoc """
  Pure mapper from admitted source records into the existing execution-attempt path.

  This module does not start workflows and does not introduce a queue. It builds
  the input/advance descriptors consumed by the existing LifecycleEvaluator and
  workflow-start outbox path.
  """

  alias Mezzanine.WorkflowExecutionLifecycleInput

  @workflow_type "execution_attempt"
  @workflow_version "execution_attempt.v1"
  @release_manifest_ref "phase3-source-admission-to-execution-attempt"
  @default_candidate_states ["Todo"]
  @default_held_states ["Backlog"]
  @default_terminal_states ["Done", "Completed", "Canceled", "Cancelled", "Duplicate"]

  @required_source_fields [
    :tenant_id,
    :installation_id,
    :installation_revision,
    :subject_id,
    :source_binding_id,
    :provider,
    :external_ref,
    :trace_id,
    :causation_id,
    :actor_ref,
    :capability
  ]

  @spec canonical_linear_source_payload(map() | struct()) :: map()
  def canonical_linear_source_payload(source) when is_map(source) do
    %{
      provider: value(source, :provider, "linear"),
      source_kind: value(source, :source_kind, "linear_issue"),
      source_binding_ref: required_string!(source, :source_binding_id),
      source_ref: source_ref(source),
      external_ref: required_string!(source, :external_ref),
      provider_revision: value(source, :provider_revision),
      title: value(source, :title),
      source_state: value(source, :source_state) || value(source, :state),
      normalized_payload: value(source, :normalized_payload, %{})
    }
    |> compact()
  end

  @spec select_dispatch_candidates([map() | struct()], keyword()) :: %{
          dispatchable: [map() | struct()],
          held: [map()]
        }
  def select_dispatch_candidates(sources, opts \\ []) when is_list(sources) and is_list(opts) do
    {dispatchable, held} =
      Enum.reduce(sources, {[], []}, fn source, {dispatchable, held} ->
        case dispatch_preflight(source, opts) do
          {:ok, _fact} ->
            {[source | dispatchable], held}

          {:error, {:dispatch_preflight_rejected, fact}} ->
            {dispatchable, [fact | held]}
        end
      end)

    %{
      dispatchable:
        dispatchable
        |> Enum.reverse()
        |> Enum.sort_by(&dispatch_sort_key/1),
      held: Enum.reverse(held)
    }
  end

  @spec dispatch_preflight(map() | struct(), keyword()) ::
          {:ok, map()} | {:error, {:dispatch_preflight_rejected, map()}}
  def dispatch_preflight(source, opts \\ []) when is_map(source) and is_list(opts) do
    source_state = source_state(source)

    candidate_states =
      normalized_state_set(Keyword.get(opts, :candidate_states, @default_candidate_states))

    held_states = normalized_state_set(Keyword.get(opts, :held_states, @default_held_states))

    terminal_states =
      normalized_state_set(Keyword.get(opts, :terminal_states, @default_terminal_states))

    known_states = MapSet.union(candidate_states, MapSet.union(held_states, terminal_states))

    cond do
      blank?(source_state) ->
        reject_preflight(source, source_state, :missing_source_state, [])

      state_member?(source_state, terminal_states) ->
        reject_preflight(source, source_state, :terminal_source_state, [])

      not state_member?(source_state, known_states) ->
        reject_preflight(source, source_state, :unknown_source_state, [])

      not state_member?(source_state, candidate_states) ->
        reject_preflight(source, source_state, :source_state_not_dispatchable, [])

      true ->
        case non_terminal_dependencies(source, terminal_states) do
          [] ->
            {:ok,
             preflight_fact(source, source_state, :dispatchable, true, [],
               fact_kind: "dispatch_preflight_accepted"
             )}

          dependencies ->
            reject_preflight(source, source_state, :non_terminal_dependency, dependencies)
        end
    end
  end

  @spec to_execution_attempt_input(map() | struct(), keyword()) ::
          {:ok, WorkflowExecutionLifecycleInput.t()} | {:error, term()}
  def to_execution_attempt_input(source, opts \\ []) when is_map(source) and is_list(opts) do
    with :ok <- validate_required_source(source),
         {:ok, _preflight_fact} <- dispatch_preflight(source, opts),
         attrs <- execution_attempt_attrs(source, opts) do
      WorkflowExecutionLifecycleInput.new(attrs)
    end
  rescue
    error in ArgumentError -> {:error, error}
  end

  @spec to_lifecycle_advance(map() | struct(), keyword()) :: {:ok, map()} | {:error, term()}
  def to_lifecycle_advance(source, opts \\ []) when is_map(source) and is_list(opts) do
    with :ok <- validate_required_source(source),
         {:ok, _preflight_fact} <- dispatch_preflight(source, opts) do
      {:ok,
       %{
         subject_id: required_string!(source, :subject_id),
         facade: Mezzanine.LifecycleEvaluator,
         handoff: :workflow_start_outbox,
         opts: [
           actor_ref: required!(source, :actor_ref),
           trace_id: required_string!(source, :trace_id),
           causation_id: required_string!(source, :causation_id),
           installation_revision: required_non_neg_integer!(source, :installation_revision),
           trigger: Keyword.get(opts, :trigger, :source_admission)
         ]
       }}
    end
  rescue
    error in ArgumentError -> {:error, error}
  end

  defp execution_attempt_attrs(source, opts) do
    tenant_id = required_string!(source, :tenant_id)
    installation_id = required_string!(source, :installation_id)
    installation_revision = required_non_neg_integer!(source, :installation_revision)
    subject_id = required_string!(source, :subject_id)
    trace_id = required_string!(source, :trace_id)
    causation_id = required_string!(source, :causation_id)
    idempotency_key = value(source, :idempotency_key) || source_idempotency_key(source)
    capability = required_string!(source, :capability)

    workflow_id =
      value(source, :workflow_id) || deterministic_workflow_id(source, idempotency_key)

    execution_id = value(source, :execution_id) || subject_id

    %{
      tenant_ref: tenant_id,
      installation_ref: "installation://#{installation_id}@#{installation_revision}",
      workspace_ref: value(source, :workspace_ref, "workspace://#{installation_id}/default"),
      project_ref: value(source, :project_ref, "project://#{installation_id}/default"),
      environment_ref: value(source, :environment_ref, "environment://default"),
      principal_ref: principal_ref(required!(source, :actor_ref)),
      system_actor_ref: "system://mezzanine/source_execution_mapper",
      resource_ref: "subject://#{subject_id}",
      subject_ref: subject_ref(source),
      workflow_id: workflow_id,
      workflow_run_id: value(source, :workflow_run_id),
      workflow_type: @workflow_type,
      workflow_version: @workflow_version,
      command_id: "source-admission:#{idempotency_key}",
      command_receipt_ref: "source-admission://#{idempotency_key}/accepted",
      workflow_input_ref: "workflow-input://source/#{idempotency_key}",
      lower_submission_ref: "jido-lower-submission://#{idempotency_key}",
      lower_idempotency_key: idempotency_key,
      activity_call_ref: "activity-call://source/#{idempotency_key}",
      authority_packet_ref: "citadel-authority-request://#{execution_id}",
      permission_decision_ref: "citadel-permission-decision://#{execution_id}",
      idempotency_key: idempotency_key,
      trace_id: trace_id,
      correlation_id: causation_id,
      release_manifest_ref: Keyword.get(opts, :release_manifest_ref, @release_manifest_ref),
      retry_policy: value(source, :retry_policy, %{"strategy" => "source_admission_default"}),
      terminal_policy: value(source, :terminal_policy, %{"receipt_required" => true}),
      routing_facts: routing_facts(source, capability, installation_revision)
    }
  end

  defp validate_required_source(source) do
    case Enum.find(@required_source_fields, &blank?(value(source, &1))) do
      nil -> :ok
      field -> {:error, {:missing_required_source_field, field}}
    end
  end

  defp reject_preflight(source, source_state, reason, dependencies) do
    {:error,
     {:dispatch_preflight_rejected,
      preflight_fact(source, source_state, reason, false, dependencies,
        fact_kind: "dispatch_preflight_rejected"
      )}}
  end

  defp preflight_fact(source, source_state, reason, eligible?, dependencies, opts) do
    %{
      "fact_kind" => Keyword.fetch!(opts, :fact_kind),
      "dispatch_eligible" => eligible?,
      "reason" => Atom.to_string(reason),
      "subject_id" => value(source, :subject_id),
      "source_ref" => source_ref_value(source),
      "source_state" => source_state,
      "provider" => value(source, :provider, "linear"),
      "provider_external_ref" =>
        value(source, :external_ref) || value(source, :provider_external_ref),
      "identifier" => value(source, :identifier),
      "priority" => value(source, :priority),
      "opened_at" => value(source, :opened_at) || value(source, :created_at),
      "dependency_refs" => Enum.map(dependencies, &dependency_projection/1)
    }
    |> compact_string_map()
  end

  defp non_terminal_dependencies(source, terminal_states) do
    source
    |> dependency_refs()
    |> Enum.reject(fn dependency ->
      dependency
      |> source_state()
      |> state_member?(terminal_states)
    end)
  end

  defp dependency_refs(source) do
    [
      value(source, :dependency_refs),
      value(source, :dependencies),
      value(source, :blocked_by),
      value(source, :blockers),
      value(source, :blocker_refs)
    ]
    |> Enum.flat_map(&List.wrap/1)
    |> Enum.filter(&is_map/1)
  end

  defp dependency_projection(dependency) do
    %{
      "provider_external_ref" =>
        value(dependency, :provider_external_ref) || value(dependency, :external_ref),
      "source_ref" => source_ref_value(dependency),
      "identifier" => value(dependency, :identifier),
      "source_state" => source_state(dependency)
    }
    |> compact_string_map()
  end

  defp dispatch_sort_key(source) do
    {
      priority_rank(value(source, :priority)),
      opened_at_rank(value(source, :opened_at) || value(source, :created_at)),
      value(source, :identifier) || value(source, :external_ref) || source_ref_value(source) || ""
    }
  end

  defp priority_rank(priority) when is_integer(priority) and priority >= 0, do: priority

  defp priority_rank(priority) when is_binary(priority) do
    case Integer.parse(String.trim(priority)) do
      {integer, ""} when integer >= 0 -> integer
      _other -> 9_999
    end
  end

  defp priority_rank(_priority), do: 9_999

  defp opened_at_rank(%DateTime{} = datetime), do: DateTime.to_unix(datetime, :microsecond)

  defp opened_at_rank(opened_at) when is_binary(opened_at) do
    case DateTime.from_iso8601(opened_at) do
      {:ok, datetime, _offset} -> DateTime.to_unix(datetime, :microsecond)
      {:error, _reason} -> 9_999_999_999_999_999
    end
  end

  defp opened_at_rank(_opened_at), do: 9_999_999_999_999_999

  defp normalized_state_set(states) do
    states
    |> List.wrap()
    |> Enum.map(&normalize_state/1)
    |> Enum.reject(&blank?/1)
    |> MapSet.new()
  end

  defp state_member?(state, states) do
    MapSet.member?(states, normalize_state(state))
  end

  defp source_state(source) when is_map(source) do
    state_name(value(source, :source_state)) ||
      state_name(value(source, :state)) ||
      state_name(value(source, :state_name)) ||
      source |> value(:issue) |> issue_state()
  end

  defp source_state(_source), do: nil

  defp issue_state(issue) when is_map(issue), do: issue |> value(:state) |> state_name()
  defp issue_state(_issue), do: nil

  defp state_name(nil), do: nil
  defp state_name(state) when is_binary(state), do: String.trim(state)
  defp state_name(%{"name" => name}) when is_binary(name), do: String.trim(name)
  defp state_name(%{name: name}) when is_binary(name), do: String.trim(name)
  defp state_name(_state), do: nil

  defp normalize_state(nil), do: nil

  defp normalize_state(state) when is_binary(state) do
    state
    |> String.trim()
    |> String.downcase()
  end

  defp normalize_state(state), do: state |> to_string() |> normalize_state()

  defp routing_facts(source, capability, installation_revision) do
    %{
      "source_binding_ref" => required_string!(source, :source_binding_id),
      "source_ref" => source_ref(source),
      "source_kind" => value(source, :source_kind, "linear_issue"),
      "provider" => value(source, :provider, "linear"),
      "provider_external_ref" => required_string!(source, :external_ref),
      "provider_revision" => value(source, :provider_revision),
      "installation_revision" => installation_revision,
      "capability" => capability,
      "capability_intent" => %{
        "capability" => capability,
        "operation" => value(source, :operation, "execute")
      },
      "actor_ref" => required!(source, :actor_ref),
      "source_payload" => canonical_linear_source_payload(source)
    }
    |> compact()
  end

  defp subject_ref(source) do
    %{
      "id" => required_string!(source, :subject_id),
      "subject_kind" => value(source, :subject_kind, "linear_coding_ticket"),
      "source_ref" => source_ref(source),
      "source_binding_ref" => required_string!(source, :source_binding_id)
    }
  end

  defp principal_ref(%{id: id, kind: kind}) when is_binary(id), do: "#{kind}://#{id}"
  defp principal_ref(%{"id" => id, "kind" => kind}) when is_binary(id), do: "#{kind}://#{id}"
  defp principal_ref(value) when is_binary(value), do: value
  defp principal_ref(value), do: raise(ArgumentError, "invalid actor_ref: #{inspect(value)}")

  defp source_ref(source) do
    value(source, :source_ref) ||
      "linear://issue/#{required_string!(source, :external_ref)}"
  end

  defp source_ref_value(source) do
    value(source, :source_ref) ||
      case value(source, :external_ref) || value(source, :provider_external_ref) do
        external_ref when is_binary(external_ref) and external_ref != "" ->
          "linear://issue/#{external_ref}"

        _missing ->
          nil
      end
  end

  defp source_idempotency_key(source) do
    [
      required_string!(source, :tenant_id),
      required_string!(source, :installation_id),
      required_string!(source, :source_binding_id),
      required_string!(source, :external_ref),
      value(source, :provider_revision, "unversioned")
    ]
    |> Enum.join(":")
  end

  defp deterministic_workflow_id(source, idempotency_key) do
    digest =
      :crypto.hash(:sha256, idempotency_key) |> Base.encode16(case: :lower) |> binary_part(0, 24)

    "execution-attempt:#{required_string!(source, :tenant_id)}:#{digest}"
  end

  defp required!(source, key) do
    case value(source, key) do
      nil -> raise ArgumentError, "missing required source field #{inspect(key)}"
      value -> value
    end
  end

  defp required_string!(source, key) do
    case required!(source, key) do
      value when is_binary(value) ->
        if String.trim(value) == "" do
          raise ArgumentError, "source field #{inspect(key)} must be a non-empty string"
        end

        value

      value ->
        raise ArgumentError,
              "source field #{inspect(key)} must be a non-empty string, got: #{inspect(value)}"
    end
  end

  defp required_non_neg_integer!(source, key) do
    case required!(source, key) do
      value when is_integer(value) and value >= 0 ->
        value

      value ->
        raise ArgumentError,
              "source field #{inspect(key)} must be a non-negative integer, got: #{inspect(value)}"
    end
  end

  defp value(source, key, default \\ nil) do
    Map.get(source, key) || Map.get(source, Atom.to_string(key)) || default
  end

  defp blank?(value), do: value in [nil, ""]

  defp compact(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp compact_string_map(map) do
    map
    |> Enum.reject(fn {_key, value} -> value in [nil, "", []] end)
    |> Map.new()
  end
end
