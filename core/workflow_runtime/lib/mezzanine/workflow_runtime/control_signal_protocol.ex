defmodule Mezzanine.WorkflowRuntime.ControlSignalProtocol do
  @moduledoc """
  Deterministic Temporal signal protocol for durable run-control commands.

  This module owns only workflow-history reduction. Canonical command, run,
  event, and delivery truth lives in Postgres through
  `Mezzanine.WorkflowRuntime.RecoveryControl`.
  """

  alias Mezzanine.OperatorWorkflowSignal
  alias Mezzanine.WorkflowDecisionTimer

  @release_manifest_ref "release://nshkr/mezzanine-recovery-control-v1"
  @registry [
    %{
      signal_name: "operator.cancel",
      signal_version: "operator-cancel.v1",
      signal_effect: "cancel_requested",
      terminal?: true
    },
    %{
      signal_name: "operator.pause",
      signal_version: "operator-pause.v1",
      signal_effect: "pause_requested",
      terminal?: false
    },
    %{
      signal_name: "operator.resume",
      signal_version: "operator-resume.v1",
      signal_effect: "resume_requested",
      terminal?: false
    },
    %{
      signal_name: "operator.retry",
      signal_version: "operator-retry.v1",
      signal_effect: "retry_requested",
      terminal?: false
    },
    %{
      signal_name: "operator.supersede",
      signal_version: "operator-supersede.v1",
      signal_effect: "supersede_requested",
      terminal?: false
    },
    %{
      signal_name: "operator.replan",
      signal_version: "operator-replan.v1",
      signal_effect: "replan_requested",
      terminal?: false
    },
    %{
      signal_name: "operator.rework",
      signal_version: "operator-rework.v1",
      signal_effect: "rework_requested",
      terminal?: false
    }
  ]

  @normalizable_keys [
    :idempotency_key,
    :last_signal_sequence,
    :release_manifest_ref,
    :seen_signal_keys,
    :signal_effect,
    :signal_name,
    :signal_sequence,
    :signal_version,
    :workflow_mode
  ]
  @key_lookup Map.new(@normalizable_keys, &{Atom.to_string(&1), &1})

  @spec registry() :: [map()]
  def registry, do: @registry

  @spec registered?(String.t(), String.t()) :: boolean()
  def registered?(signal_name, signal_version) do
    Enum.any?(@registry, fn entry ->
      entry.signal_name == signal_name and entry.signal_version == signal_version
    end)
  end

  @spec signal_for_action(atom()) :: {:ok, map()} | {:error, term()}
  def signal_for_action(:deadline), do: signal_for_action(:cancel)

  def signal_for_action(action) do
    signal_name = "operator." <> Atom.to_string(action)

    case Enum.find(@registry, &(&1.signal_name == signal_name)) do
      nil -> {:error, {:unregistered_control_action, action}}
      entry -> {:ok, entry}
    end
  end

  @spec build_signal(map() | keyword()) :: {:ok, OperatorWorkflowSignal.t()} | {:error, term()}
  def build_signal(attrs) do
    attrs = normalize(attrs)

    with {:ok, entry} <-
           registry_entry(Map.get(attrs, :signal_name), Map.get(attrs, :signal_version)) do
      attrs
      |> Map.put_new(:signal_effect, entry.signal_effect)
      |> Map.put_new(:release_manifest_ref, @release_manifest_ref)
      |> OperatorWorkflowSignal.new()
    end
  end

  @spec decision_timer(map() | keyword()) :: {:ok, WorkflowDecisionTimer.t()} | {:error, term()}
  def decision_timer(attrs) do
    attrs
    |> normalize()
    |> Map.put_new(:release_manifest_ref, @release_manifest_ref)
    |> WorkflowDecisionTimer.new()
  end

  @spec run_decision_review(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def run_decision_review(attrs) do
    with {:ok, timer} <- decision_timer(attrs) do
      {:ok,
       %{
         workflow_id: timer.workflow_id,
         workflow_run_id: timer.workflow_run_id,
         workflow_state: "awaiting_decision_or_timer",
         timer_ref: "workflow-timer://#{timer.workflow_id}/#{timer.timer_id}",
         timer_id: timer.timer_id,
         timer_state: timer.timer_state,
         timer_duration_ms: timer.timer_duration_ms,
         workflow_history_ref: timer.workflow_history_ref,
         projection_ref: timer.projection_ref,
         trace_id: timer.trace_id,
         release_manifest_ref: timer.release_manifest_ref,
         history_policy: "temporal_timer_history"
       }}
    end
  end

  @doc "Initial deterministic state retained in Temporal workflow history."
  @spec initial_workflow_state() :: map()
  def initial_workflow_state do
    %{
      workflow_mode: "running",
      last_signal_sequence: 0,
      seen_signal_keys: MapSet.new(),
      ordering_state: "ready"
    }
  end

  @doc "Reduces a versioned signal without becoming canonical recovery truth."
  @spec reduce_signal(map(), map() | keyword()) :: {:ok, map()} | {:error, term()}
  def reduce_signal(state, attrs) when is_map(state) do
    with {:ok, signal} <- build_signal(attrs) do
      seen = Map.get(state, :seen_signal_keys, MapSet.new())

      cond do
        MapSet.member?(seen, signal.idempotency_key) ->
          {:ok, Map.put(state, :ordering_state, "duplicate_suppressed")}

        signal.signal_sequence <= Map.get(state, :last_signal_sequence, 0) ->
          {:ok, Map.put(state, :ordering_state, "out_of_order_rejected")}

        signal.signal_name == "operator.resume" and Map.get(state, :workflow_mode) != "paused" ->
          {:ok, Map.put(state, :ordering_state, "resume_without_pause_rejected")}

        true ->
          {:ok,
           state
           |> Map.put(:workflow_mode, workflow_mode_after(signal.signal_name))
           |> Map.put(:last_signal_sequence, signal.signal_sequence)
           |> Map.put(:seen_signal_keys, MapSet.put(seen, signal.idempotency_key))
           |> Map.put(:ordering_state, "applied")}
      end
    end
  end

  defp registry_entry(signal_name, signal_version) do
    case Enum.find(@registry, fn entry ->
           entry.signal_name == signal_name and entry.signal_version == signal_version
         end) do
      nil -> {:error, {:unregistered_signal, signal_name, signal_version}}
      entry -> {:ok, entry}
    end
  end

  defp workflow_mode_after("operator.cancel"), do: "cancel_requested"
  defp workflow_mode_after("operator.pause"), do: "paused"
  defp workflow_mode_after("operator.resume"), do: "running"
  defp workflow_mode_after("operator.retry"), do: "retry_requested"
  defp workflow_mode_after("operator.supersede"), do: "supersede_requested"
  defp workflow_mode_after("operator.replan"), do: "replan_requested"
  defp workflow_mode_after("operator.rework"), do: "rework_requested"

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_keys()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize_keys()
  defp normalize(map) when is_map(map), do: normalize_keys(map)

  defp normalize_keys(map) do
    Map.new(map, fn {key, value} -> {normalize_key(key), value} end)
  end

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: Map.get(@key_lookup, key, key)
end
