defmodule Mezzanine.WorkflowRuntime.WorkflowLifecycleCompensation do
  @moduledoc """
  Phase 5 workflow-lifecycle compensation routing profile.

  Workflow lifecycle compensation is owned by the Temporal workflow boundary.
  This module builds public-safe signal and activity routes for that boundary;
  it does not mutate execution, decision, audit, lower, archival, or projection
  state directly.
  """

  @release_manifest_ref "phase5-v7-m02ac-workflow-lifecycle-compensation-routing"

  @signal_routes %{
    retry: %{
      signal_name: "workflow.compensation.retry",
      signal_version: "workflow-compensation-retry.v1",
      effect: :retry_workflow_lifecycle
    },
    cancel: %{
      signal_name: "workflow.compensation.cancel",
      signal_version: "workflow-compensation-cancel.v1",
      effect: :cancel_workflow_lifecycle
    },
    operator_retry: %{
      signal_name: "workflow.compensation.operator_retry",
      signal_version: "workflow-compensation-operator-retry.v1",
      effect: :operator_retry_workflow_lifecycle
    }
  }

  @activity_routes %{
    cancel: %{
      activity_name: "compensate_cancelled_run",
      activity_module: Mezzanine.Activities.CompensateCancelledRun,
      activity_version: "compensate-cancelled-run.v1",
      task_queue: "mezzanine.hazmat"
    }
  }

  @forbidden_target_kinds ["owner_command", "local_mutation", "lifecycle_continuation_handler"]
  @required_target_fields ["workflow_id", "idempotency_key"]

  @spec profile() :: map()
  def profile do
    %{
      profile_name: "Mezzanine.WorkflowLifecycleCompensation.v1",
      owner_repo: :mezzanine,
      owner_package: :workflow_runtime,
      compensation_owner: :workflow_lifecycle,
      route_targets: [:workflow_signal, :workflow_activity],
      signal_boundary: Mezzanine.WorkflowRuntime,
      signal_routes: @signal_routes,
      activity_routes: @activity_routes,
      forbidden_target_kinds: @forbidden_target_kinds,
      lifecycle_continuation_role: :retry_dead_letter_visibility_only,
      workflow_truth_owner: :temporal,
      release_manifest_ref: @release_manifest_ref
    }
  end

  @spec release_manifest_ref() :: String.t()
  def release_manifest_ref, do: @release_manifest_ref

  @spec route(map()) :: {:ok, map()} | {:error, term()}
  def route(attrs) when is_map(attrs) do
    with :ok <- ensure_workflow_owner(attrs),
         {:ok, compensation_kind} <- compensation_kind(attrs),
         {:ok, target} <- owner_target(attrs) do
      route_target(compensation_kind, target, attrs)
    end
  end

  def route(_attrs), do: {:error, :invalid_workflow_lifecycle_compensation}

  @spec dispatch_signal(map(), module()) :: {:ok, map()} | {:error, term()}
  def dispatch_signal(attrs, runtime \\ Mezzanine.WorkflowRuntime) do
    case route(attrs) do
      {:ok, %{route_kind: :workflow_signal, request: request} = route} ->
        with {:ok, receipt} <- runtime.signal_workflow(request) do
          {:ok, Map.put(route, :runtime_receipt, sanitize_runtime_receipt(receipt))}
        end

      {:ok, %{route_kind: :workflow_activity}} ->
        {:error, :workflow_activity_must_run_inside_temporal_workflow}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_workflow_owner(attrs) do
    case field_value(attrs, :compensation_owner) do
      owner when owner in [:workflow_lifecycle, "workflow_lifecycle"] -> :ok
      owner -> {:error, {:invalid_compensation_owner, owner}}
    end
  end

  defp compensation_kind(attrs) do
    case normalize_kind(field_value(attrs, :compensation_kind)) do
      {:ok, kind} when is_map_key(@signal_routes, kind) or is_map_key(@activity_routes, kind) ->
        {:ok, kind}

      {:ok, kind} ->
        {:error, {:unsupported_workflow_lifecycle_compensation_kind, kind}}

      {:error, kind} ->
        {:error, {:invalid_compensation_kind, kind}}
    end
  end

  defp owner_target(attrs) do
    case field_value(attrs, :owner_command_or_signal) do
      %{} = target -> {:ok, target}
      _missing -> {:error, :missing_owner_command_or_signal}
    end
  end

  defp route_target(compensation_kind, target, attrs) do
    case target_kind(target) do
      "workflow_signal" ->
        signal_route(compensation_kind, target, attrs)

      "workflow_activity" ->
        activity_route(compensation_kind, target, attrs)

      forbidden when forbidden in @forbidden_target_kinds ->
        {:error, {:forbidden_target_kind, forbidden}}

      other ->
        {:error, {:unsupported_workflow_lifecycle_target, other}}
    end
  end

  defp signal_route(compensation_kind, target, attrs) do
    route = Map.fetch!(@signal_routes, compensation_kind)

    with :ok <- require_target_fields(target, ["signal" | @required_target_fields]),
         :ok <- require_expected_signal(target, route.signal_name) do
      signal_id =
        target_value(target, "signal_id") ||
          "compensation:#{field_value(attrs, :compensation_ref)}"

      request =
        %{
          workflow_id: target_value(target, "workflow_id"),
          workflow_run_id: target_value(target, "workflow_run_id"),
          signal_id: signal_id,
          signal_name: route.signal_name,
          signal_version: route.signal_version,
          signal_effect: route.effect,
          signal_payload_ref:
            target_value(target, "signal_payload_ref") ||
              "workflow-compensation://#{target_value(target, "workflow_id")}/#{signal_id}",
          signal_payload_hash:
            target_value(target, "signal_payload_hash") ||
              "sha256:#{target_value(target, "idempotency_key")}",
          idempotency_key: target_value(target, "idempotency_key"),
          tenant_ref: field_value(attrs, :tenant_id),
          installation_ref: field_value(attrs, :installation_id),
          resource_ref: field_value(attrs, :source_event_ref),
          compensation_ref: field_value(attrs, :compensation_ref),
          source_context: field_value(attrs, :source_context),
          source_event_ref: field_value(attrs, :source_event_ref),
          failed_step_ref: field_value(attrs, :failed_step_ref),
          precondition: field_value(attrs, :precondition),
          side_effect_scope: field_value(attrs, :side_effect_scope),
          dead_letter_ref: field_value(attrs, :dead_letter_ref),
          operator_action_ref: field_value(attrs, :operator_action_ref),
          audit_or_evidence_ref: field_value(attrs, :audit_or_evidence_ref),
          trace_id: field_value(attrs, :trace_id),
          causation_id: field_value(attrs, :causation_id),
          release_manifest_ref: field_value(attrs, :release_manifest_ref) || @release_manifest_ref
        }
        |> drop_nil_values()

      {:ok,
       %{
         route_kind: :workflow_signal,
         compensation_owner: :workflow_lifecycle,
         compensation_kind: compensation_kind,
         signal_boundary: Mezzanine.WorkflowRuntime,
         lifecycle_continuation_role: :retry_dead_letter_visibility_only,
         request: request
       }}
    end
  end

  defp activity_route(compensation_kind, target, attrs) do
    route = Map.get(@activity_routes, compensation_kind)

    with %{} <- route || {:error, {:unsupported_activity_compensation_kind, compensation_kind}},
         :ok <- require_target_fields(target, ["activity" | @required_target_fields]),
         :ok <- require_expected_activity(target, route.activity_name) do
      input =
        %{
          workflow_id: target_value(target, "workflow_id"),
          workflow_run_id: target_value(target, "workflow_run_id"),
          idempotency_key: target_value(target, "idempotency_key"),
          tenant_ref: field_value(attrs, :tenant_id),
          installation_ref: field_value(attrs, :installation_id),
          compensation_ref: field_value(attrs, :compensation_ref),
          source_event_ref: field_value(attrs, :source_event_ref),
          failed_step_ref: field_value(attrs, :failed_step_ref),
          precondition: field_value(attrs, :precondition),
          side_effect_scope: field_value(attrs, :side_effect_scope),
          trace_id: field_value(attrs, :trace_id),
          causation_id: field_value(attrs, :causation_id),
          release_manifest_ref: field_value(attrs, :release_manifest_ref) || @release_manifest_ref
        }
        |> drop_nil_values()

      {:ok,
       %{
         route_kind: :workflow_activity,
         compensation_owner: :workflow_lifecycle,
         compensation_kind: compensation_kind,
         activity_owner: :workflow_lifecycle,
         activity_name: route.activity_name,
         activity_module: route.activity_module,
         activity_version: route.activity_version,
         task_queue: route.task_queue,
         lifecycle_continuation_role: :retry_dead_letter_visibility_only,
         input: input
       }}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp require_target_fields(target, fields) do
    missing =
      fields
      |> Enum.reject(&present?(target_value(target, &1)))

    case missing do
      [] -> :ok
      _ -> {:error, {:missing_target_fields, missing}}
    end
  end

  defp require_expected_signal(target, expected_signal) do
    case target_value(target, "signal") do
      ^expected_signal -> :ok
      actual -> {:error, {:unexpected_workflow_signal, actual}}
    end
  end

  defp require_expected_activity(target, expected_activity) do
    case target_value(target, "activity") do
      ^expected_activity -> :ok
      actual -> {:error, {:unexpected_workflow_activity, actual}}
    end
  end

  defp normalize_kind(kind) when kind in [:retry, :cancel, :operator_retry], do: {:ok, kind}

  defp normalize_kind(kind) when is_binary(kind) do
    case kind do
      "retry" -> {:ok, :retry}
      "cancel" -> {:ok, :cancel}
      "operator_retry" -> {:ok, :operator_retry}
      _ -> {:error, kind}
    end
  end

  defp normalize_kind(kind), do: {:error, kind}

  defp target_kind(target), do: target_value(target, "kind")

  defp field_value(attrs, field),
    do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))

  defp target_value(target, field) when is_binary(field),
    do: Map.get(target, field) || Map.get(target, String.to_atom(field))

  defp sanitize_runtime_receipt(receipt) do
    receipt
    |> normalize_receipt()
    |> Map.drop([:raw_temporalex_result, :temporalex_struct, :raw_history_event, :task_token])
  end

  defp normalize_receipt(%_{} = struct), do: Map.from_struct(struct)
  defp normalize_receipt(receipt) when is_map(receipt), do: receipt

  defp drop_nil_values(map) do
    map
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(nil), do: false
  defp present?(_value), do: true
end
