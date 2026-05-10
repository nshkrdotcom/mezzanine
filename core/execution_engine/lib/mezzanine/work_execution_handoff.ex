defmodule Mezzanine.WorkExecutionHandoff do
  @moduledoc """
  Bridges accepted WorkControl runs to the execution ledger.

  WorkControl owns the accepted run and review rows. WorkflowStartHandoff owns
  the durable workflow-start outbox. This module creates the matching current
  execution row so readback and leases can refer to the same run without waiting
  for a lower provider submission.
  """

  require Ash.Query

  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.ServiceSupport

  @type handoff_result :: %{
          required(:status) => :created | :reused,
          required(:execution) => ExecutionRecord.t()
        }

  @spec ensure_current_execution(String.t(), map(), map(), map() | keyword()) ::
          {:ok, handoff_result()} | {:error, term()}
  def ensure_current_execution(tenant_id, started_run, workflow_handoff, attrs)
      when is_binary(tenant_id) and is_map(started_run) and is_map(workflow_handoff) do
    attrs = Map.new(attrs)

    with {:ok, execution_attrs} <-
           execution_attrs(tenant_id, started_run, workflow_handoff, attrs),
         {:ok, existing_execution} <- fetch_existing_execution(execution_attrs) do
      case existing_execution do
        %ExecutionRecord{} = execution ->
          {:ok, %{status: :reused, execution: execution}}

        nil ->
          create_execution(execution_attrs)
      end
    end
  end

  defp create_execution(execution_attrs) do
    case ExecutionRecord.dispatch(execution_attrs) do
      {:ok, %ExecutionRecord{} = execution} ->
        {:ok, %{status: :created, execution: execution}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_existing_execution(%{
         installation_id: installation_id,
         submission_dedupe_key: submission_dedupe_key
       }) do
    ExecutionRecord
    |> Ash.Query.filter(
      installation_id == ^installation_id and submission_dedupe_key == ^submission_dedupe_key
    )
    |> Ash.read(authorize?: false, domain: Mezzanine.Execution)
    |> case do
      {:ok, [execution | _]} -> {:ok, execution}
      {:ok, []} -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp execution_attrs(tenant_id, started_run, workflow_handoff, attrs) do
    work_object = Map.fetch!(started_run, :work_object)
    plan = Map.fetch!(started_run, :plan)
    run = Map.fetch!(started_run, :run)
    review_unit = Map.get(started_run, :review_unit)

    with {:ok, installation_id} <- installation_id(tenant_id, workflow_handoff, attrs),
         {:ok, submission_dedupe_key} <-
           submission_dedupe_key(started_run, workflow_handoff, attrs),
         {:ok, trace_id} <- required_string(attrs, :trace_id, :missing_trace_id),
         {:ok, recipe_ref} <- recipe_ref(plan, attrs) do
      {:ok,
       %{
         tenant_id: tenant_id,
         installation_id: installation_id,
         subject_id: work_object.id,
         recipe_ref: recipe_ref,
         compiled_pack_revision: compiled_pack_revision(attrs),
         binding_snapshot: binding_snapshot(work_object, run, attrs),
         dispatch_envelope: dispatch_envelope(started_run, workflow_handoff, attrs),
         intent_snapshot: intent_snapshot(plan, run, review_unit, workflow_handoff),
         submission_dedupe_key: submission_dedupe_key,
         trace_id: trace_id,
         causation_id: causation_id(workflow_handoff, attrs, run),
         actor_ref: actor_ref(attrs)
       }}
    end
  end

  defp installation_id(tenant_id, workflow_handoff, attrs) do
    case map_value(attrs, :installation_ref) ||
           row_value(workflow_handoff, :installation_ref) ||
           "installation://#{tenant_id}/default" do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, :missing_installation_ref}
    end
  end

  defp submission_dedupe_key(started_run, workflow_handoff, attrs) do
    value =
      map_value(attrs, :idempotency_key) ||
        row_value(workflow_handoff, :idempotency_key) ||
        "mezzanine-run:#{Map.fetch!(started_run, :run).id}"

    case value do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, :missing_submission_dedupe_key}
    end
  end

  defp recipe_ref(plan, attrs) do
    value =
      map_value(attrs, :recipe_ref) ||
        plan |> first_run_intent() |> map_value(:intent_id) ||
        "work_control_run"

    case value do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, :missing_recipe_ref}
    end
  end

  defp binding_snapshot(work_object, run, attrs) do
    run_profile = Map.get(run, :runtime_profile) || %{}

    %{
      "work_object_id" => work_object.id,
      "program_id" => work_object.program_id,
      "work_class_id" => work_object.work_class_id,
      "source_kind" => work_object.source_kind,
      "external_ref" => work_object.external_ref,
      "run_id" => run.id,
      "runtime_profile" => normalize_map(run_profile),
      "grant_profile" => normalize_map(Map.get(run, :grant_profile) || %{}),
      "runtime_profile_ref" => map_value(run_profile, :runtime_profile_ref),
      "runtime_profile_kind" => map_value(run_profile, :runtime_profile_kind),
      "lower_runtime_kind" => map_value(run_profile, :lower_runtime_kind),
      "source_binding_refs" => list_value(attrs, :source_binding_refs),
      "resource_scope_refs" => list_value(attrs, :resource_scope_refs),
      "workspace_policy_ref" => map_value(attrs, :workspace_policy_ref)
    }
    |> compact_map()
  end

  defp dispatch_envelope(started_run, workflow_handoff, attrs) do
    plan = Map.fetch!(started_run, :plan)
    run = Map.fetch!(started_run, :run)
    run_profile = Map.get(run, :runtime_profile) || %{}
    intent = first_run_intent(plan)
    outbox_row = map_value(workflow_handoff, :outbox_row) || %{}

    %{
      "dispatch_boundary" => "mezzanine_work_execution_handoff",
      "workflow_start_ref" => map_value(workflow_handoff, :workflow_start_ref),
      "workflow_start_evidence_ref" => map_value(workflow_handoff, :evidence_ref),
      "workflow_start_outbox_id" => map_value(outbox_row, :outbox_id),
      "workflow_id" => map_value(outbox_row, :workflow_id),
      "workflow_dispatch_state" => map_value(outbox_row, :dispatch_state),
      "command_receipt_ref" => map_value(outbox_row, :command_receipt_ref),
      "authority_packet_ref" => map_value(outbox_row, :authority_packet_ref),
      "permission_decision_ref" => map_value(outbox_row, :permission_decision_ref),
      "workflow_input_ref" => map_value(outbox_row, :workflow_input_ref),
      "workflow_type" => map_value(outbox_row, :workflow_type),
      "run_id" => run.id,
      "run_intent_ref" => map_value(intent, :intent_id),
      "recipe_ref" => map_value(attrs, :recipe_ref) || map_value(intent, :intent_id),
      "runtime_class" =>
        map_value(intent, :runtime_class) || map_value(run_profile, :runtime_class),
      "capability" => map_value(intent, :capability) || map_value(run_profile, :capability_id),
      "lower_runtime_kind" => map_value(run_profile, :lower_runtime_kind),
      "requested_action_ids" => map_value(run_profile, :requested_action_ids),
      "requested_capability_ids" => map_value(run_profile, :requested_capability_ids),
      "live_provider_allowed" => map_value(run_profile, :live_provider_allowed)
    }
    |> compact_map()
  end

  defp intent_snapshot(plan, run, review_unit, workflow_handoff) do
    %{
      "plan_id" => plan.id,
      "run_id" => run.id,
      "run_status" => run.status,
      "run_intent" => normalize_map(first_run_intent(plan)),
      "review_required" => plan.derived_review_intents != [],
      "review_unit_id" => review_unit && review_unit.id,
      "workflow_start_ref" => map_value(workflow_handoff, :workflow_start_ref),
      "workflow_start_evidence_ref" => map_value(workflow_handoff, :evidence_ref)
    }
    |> compact_map()
  end

  defp compiled_pack_revision(attrs) do
    case map_value(attrs, :compiled_pack_revision) || map_value(attrs, :pack_revision) do
      value when is_integer(value) and value > 0 ->
        value

      value when is_binary(value) ->
        case Integer.parse(value) do
          {integer, ""} when integer > 0 -> integer
          _other -> 1
        end

      _other ->
        1
    end
  end

  defp causation_id(workflow_handoff, attrs, run) do
    map_value(attrs, :causation_id) ||
      map_value(workflow_handoff, :workflow_start_ref) ||
      "run:#{run.id}"
  end

  defp actor_ref(attrs) do
    case map_value(attrs, :actor_ref) do
      %{id: id, kind: kind} when is_binary(id) ->
        %{"kind" => normalize_atom(kind), "ref" => id}

      %{"id" => id, "kind" => kind} when is_binary(id) ->
        %{"kind" => normalize_atom(kind), "ref" => id}

      value when is_binary(value) and value != "" ->
        %{"kind" => "human", "ref" => value}

      %{} = map ->
        normalize_map(map)

      _other ->
        %{"kind" => "system", "ref" => "work_execution_handoff"}
    end
  end

  defp required_string(attrs, key, error) do
    case map_value(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, error}
    end
  end

  defp first_run_intent(%{derived_run_intents: [intent | _]}), do: intent
  defp first_run_intent(_plan), do: %{}

  defp row_value(workflow_handoff, key) do
    workflow_handoff
    |> map_value(:outbox_row)
    |> map_value(key)
  end

  defp list_value(attrs, key) do
    case map_value(attrs, key) do
      value when is_list(value) -> value
      nil -> nil
      value -> [value]
    end
  end

  defp map_value(map, key), do: ServiceSupport.map_value(map, key)

  defp compact_map(map), do: Map.reject(map, fn {_key, value} -> value in [nil, [], %{}] end)

  defp normalize_map(map) when is_map(map) do
    map
    |> Map.new(fn {key, value} -> {normalize_key(key), normalize_value(value)} end)
    |> compact_map()
  end

  defp normalize_map(_value), do: %{}

  defp normalize_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp normalize_value(%NaiveDateTime{} = value), do: NaiveDateTime.to_iso8601(value)
  defp normalize_value(value) when is_boolean(value), do: value
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value), do: value

  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key), do: key

  defp normalize_atom(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_atom(value) when is_binary(value), do: value
  defp normalize_atom(_value), do: "unknown"
end
