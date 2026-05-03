defmodule Mezzanine.WorkflowRuntime.TemporalexAdapter do
  @moduledoc """
  Concrete Temporalex-backed implementation of `Mezzanine.WorkflowRuntime`.

  Public callers continue to receive Mezzanine DTOs. Temporalex handles, raw
  SDK errors, and history payloads stay inside the workflow-runtime boundary.
  """

  @behaviour Mezzanine.WorkflowRuntime

  alias Mezzanine.GovernedRuntimeConfig
  alias Mezzanine.WorkflowCancelReceipt
  alias Mezzanine.WorkflowDescription
  alias Mezzanine.WorkflowHistoryRef
  alias Mezzanine.WorkflowQueryResult
  alias Mezzanine.WorkflowRuntime.TemporalexBoundary
  alias Mezzanine.WorkflowRuntime.TemporalSupervisor
  alias Mezzanine.WorkflowSignalReceiptResult
  alias Mezzanine.WorkflowStartReceipt

  @default_timeout_ms 10_000
  @normalizable_keys [
    :args,
    :authority_packet_ref,
    :command_id,
    :connection,
    :correlation_id,
    :id,
    :idempotency_key,
    :memo,
    :operation,
    :permission_decision_ref,
    :query_name,
    :raw_history_event,
    :raw_temporalex_result,
    :reason,
    :release_manifest_ref,
    :resource_ref,
    :retry_policy,
    :run_id,
    :search_attributes,
    :signal_id,
    :signal_name,
    :signal_payload_hash,
    :signal_payload_ref,
    :signal_version,
    :state_ref,
    :status,
    :task_queue,
    :task_token,
    :temporalex_boundary,
    :temporal_connection,
    :temporalex_struct,
    :tenant_ref,
    :timeout_ms,
    :trace_id,
    :workflow_id,
    :workflow_module,
    :workflow_run_id,
    :workflow_type,
    :workflow_version
  ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @impl true
  def start_workflow(request) do
    request = normalize(request)

    with {:ok, workflow_id} <- required(request, :workflow_id),
         {:ok, workflow_module} <- required(request, :workflow_module),
         {:ok, task_queue} <- required(request, :task_queue),
         {:ok, args} <- required(request, :args),
         {:ok, handle} <-
           boundary(request).start_workflow(
             connection_for(request),
             workflow_module,
             args,
             start_opts(request, workflow_id, task_queue)
           ) do
      start_receipt(request, handle)
    else
      {:error, reason} -> {:error, normalize_error(reason, request)}
    end
  end

  @impl true
  def signal_workflow(request) do
    request = normalize(request)

    with {:ok, workflow_id} <- required(request, :workflow_id),
         {:ok, signal_name} <- required(request, :signal_name),
         {:ok, _receipt} <-
           normalize_ok(
             boundary(request).signal_workflow(
               connection_for(request),
               workflow_id,
               signal_name,
               signal_args(request),
               call_opts(request)
             )
           ) do
      {:ok,
       %WorkflowSignalReceiptResult{
         signal_ref: signal_ref(request, workflow_id, signal_name),
         status: "delivered_to_temporal",
         trace_id: Map.get(request, :trace_id),
         failure_class: "none"
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason, request)}
    end
  end

  @impl true
  def query_workflow(request) do
    request = normalize(request)

    with {:ok, workflow_id} <- required(request, :workflow_id),
         {:ok, query_name} <- required(request, :query_name),
         {:ok, result} <-
           boundary(request).query_workflow(
             connection_for(request),
             workflow_id,
             query_name,
             query_args(request),
             call_opts(request)
           ) do
      {:ok,
       %WorkflowQueryResult{
         workflow_ref: workflow_ref(workflow_id, run_id(request)),
         query_name: query_name,
         state_ref: query_state_ref(workflow_id, query_name, result),
         summary: compact_summary(result),
         trace_id: Map.get(request, :trace_id),
         failure_class: "none"
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason, request)}
    end
  end

  @impl true
  def cancel_workflow(request) do
    request = normalize(request)

    with {:ok, workflow_id} <- required(request, :workflow_id),
         {:ok, _receipt} <-
           normalize_ok(
             boundary(request).cancel_workflow(
               connection_for(request),
               workflow_id,
               call_opts(request)
             )
           ) do
      {:ok,
       %WorkflowCancelReceipt{
         workflow_ref: workflow_ref(workflow_id, run_id(request)),
         status: "cancel_requested",
         trace_id: Map.get(request, :trace_id),
         failure_class: "none"
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason, request)}
    end
  end

  @impl true
  def describe_workflow(request) do
    request = normalize(request)

    with {:ok, workflow_id} <- required(request, :workflow_id),
         {:ok, info} <-
           boundary(request).describe_workflow(
             connection_for(request),
             workflow_id,
             call_opts(request)
           ) do
      {:ok,
       %WorkflowDescription{
         workflow_ref: workflow_ref(workflow_id, description_run_id(info, request)),
         status: description_status(info),
         search_attributes: safe_search_attributes(info),
         trace_id: Map.get(request, :trace_id),
         failure_class: "none"
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason, request)}
    end
  end

  @impl true
  def fetch_workflow_history_ref(request) do
    request = normalize(request)

    with {:ok, workflow_id} <- required(request, :workflow_id),
         {:ok, description} <- describe_workflow(request) do
      material = :erlang.term_to_binary({workflow_id, run_id(request), description.status})

      {:ok,
       %WorkflowHistoryRef{
         workflow_ref: workflow_ref(workflow_id, run_id(request)),
         history_ref: "temporal-history://#{workflow_id}/#{run_id(request) || "latest"}",
         history_hash: "sha256:" <> Base.encode16(:crypto.hash(:sha256, material), case: :lower),
         trace_id: Map.get(request, :trace_id)
       }}
    else
      {:error, reason} -> {:error, normalize_error(reason, request)}
    end
  end

  defp boundary(request) do
    GovernedRuntimeConfig.module(
      request,
      :mezzanine_workflow_runtime,
      :temporalex_boundary,
      TemporalexBoundary,
      governed_default?: true
    )
  end

  defp connection_for(request) do
    Map.get(request, :connection) ||
      Map.get(request, :temporal_connection) ||
      TemporalSupervisor.connection_name(Map.get(request, :task_queue, "mezzanine.agentic"))
  end

  defp start_opts(request, workflow_id, task_queue) do
    request
    |> Map.take([:search_attributes, :memo, :retry_policy])
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.into([])
    |> Keyword.put(:id, workflow_id)
    |> Keyword.put(:task_queue, task_queue)
    |> Keyword.put(:timeout, Map.get(request, :timeout_ms, @default_timeout_ms))
  end

  defp call_opts(request) do
    []
    |> Keyword.put(:timeout, Map.get(request, :timeout_ms, @default_timeout_ms))
    |> maybe_put(:run_id, run_id(request))
    |> maybe_put(:reason, Map.get(request, :reason))
  end

  defp start_receipt(request, handle) do
    args = normalize(Map.get(request, :args, %{}))
    workflow_id = Map.fetch!(request, :workflow_id)
    run_id = Map.get(handle, :run_id)

    WorkflowStartReceipt.new(%{
      workflow_ref: workflow_ref(workflow_id, run_id),
      workflow_id: workflow_id,
      workflow_run_id: run_id,
      workflow_type: Map.get(request, :workflow_type, workflow_type(request)),
      workflow_version: Map.get(request, :workflow_version, "temporalex.v1"),
      tenant_ref: Map.get(request, :tenant_ref) || Map.get(args, :tenant_ref),
      resource_ref: Map.get(request, :resource_ref) || Map.get(args, :resource_ref),
      command_id: Map.get(request, :command_id) || Map.get(args, :command_id),
      idempotency_key: Map.get(request, :idempotency_key),
      trace_id: Map.get(request, :trace_id) || Map.get(args, :trace_id),
      correlation_id: Map.get(request, :correlation_id) || Map.get(args, :correlation_id),
      release_manifest_ref: Map.get(request, :release_manifest_ref),
      start_state: "started",
      duplicate?: false,
      retry_class: "none",
      failure_class: "none"
    })
  end

  defp signal_args(request) do
    Map.take(request, [
      :signal_id,
      :signal_version,
      :signal_payload_ref,
      :signal_payload_hash,
      :idempotency_key,
      :tenant_ref,
      :resource_ref,
      :authority_packet_ref,
      :permission_decision_ref,
      :trace_id,
      :correlation_id,
      :release_manifest_ref
    ])
  end

  defp query_args(request) do
    request
    |> Map.drop([
      :operation,
      :workflow_id,
      :workflow_run_id,
      :run_id,
      :query_name,
      :connection,
      :temporal_connection,
      :timeout_ms
    ])
  end

  defp required(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} when value not in [nil, ""] -> {:ok, value}
      _missing -> {:error, {:invalid_request, {:missing_required_field, key}}}
    end
  end

  defp normalize_ok(:ok), do: {:ok, :ok}
  defp normalize_ok({:ok, value}), do: {:ok, value}
  defp normalize_ok({:error, reason}), do: {:error, reason}

  defp workflow_ref(workflow_id, nil), do: "temporal-workflow://#{workflow_id}"
  defp workflow_ref(workflow_id, ""), do: workflow_ref(workflow_id, nil)
  defp workflow_ref(workflow_id, run_id), do: "temporal-workflow://#{workflow_id}/#{run_id}"

  defp signal_ref(request, workflow_id, signal_name) do
    "temporal-signal://#{workflow_id}/#{Map.get(request, :signal_id, signal_name)}"
  end

  defp query_state_ref(workflow_id, query_name, result) do
    result
    |> normalize()
    |> Map.get(:state_ref, "temporal-query://#{workflow_id}/#{query_name}")
  end

  defp compact_summary(result) when is_map(result) do
    result
    |> normalize()
    |> Map.drop([:raw_temporalex_result, :temporalex_struct, :raw_history_event, :task_token])
  end

  defp compact_summary(result), do: %{value: result}

  defp description_status(info) do
    info
    |> normalize()
    |> Map.get(:status, "unknown")
  end

  defp description_run_id(info, request) do
    info
    |> normalize()
    |> Map.get(:run_id, run_id(request))
  end

  defp safe_search_attributes(info) do
    info
    |> normalize()
    |> Map.get(:search_attributes, %{})
    |> drop_forbidden_keys()
  end

  defp run_id(request), do: Map.get(request, :run_id) || Map.get(request, :workflow_run_id)

  defp workflow_type(request) do
    request
    |> Map.fetch!(:workflow_module)
    |> Module.split()
    |> List.last()
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_keys()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize_keys()
  defp normalize(map) when is_map(map), do: normalize_keys(map)

  defp normalize_keys(map) do
    map
    |> Enum.map(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.new()
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)

  defp normalize_error({:temporalex, reason}, request), do: normalize_error(reason, request)

  defp normalize_error({:already_started, _existing_ref} = reason, _request), do: reason

  defp normalize_error({:invalid_request, _reason} = reason, _request), do: reason

  defp normalize_error(:timeout, _request), do: {:temporal_unavailable, :timeout}

  defp normalize_error({:connection_error, reason}, _request),
    do: {:temporal_unavailable, reason}

  defp normalize_error(reason, request) do
    if duplicate_start_error?(reason) do
      {:already_started, workflow_ref(Map.get(request, :workflow_id, "unknown"), run_id(request))}
    else
      {:temporal_unavailable, reason}
    end
  end

  defp duplicate_start_error?(reason) do
    reason
    |> inspect()
    |> String.downcase()
    |> String.contains?("already")
  end

  defp drop_forbidden_keys(map) when is_map(map) do
    Map.drop(map, [
      :raw_temporalex_result,
      :temporalex_struct,
      :raw_history_event,
      :task_token,
      "raw_temporalex_result",
      "temporalex_struct",
      "raw_history_event",
      "task_token"
    ])
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, _key, ""), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)
end
