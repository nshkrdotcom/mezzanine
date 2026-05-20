defmodule Mezzanine.Core.GovernedEffects.DiagnosticWorkflow do
  @moduledoc """
  Non-coding diagnostic proof workflow for governed effects.
  """

  alias Mezzanine.Core.GovernedEffects.Coordinator

  @operations %{
    echo: %{effect_type: "diagnostic.echo", operation: "echo"},
    probe: %{effect_type: "diagnostic.probe", operation: "probe"}
  }

  @spec registered_effect_type?(String.t()) :: boolean()
  def registered_effect_type?(effect_type) when is_binary(effect_type) do
    effect_type in Enum.map(Map.values(@operations), & &1.effect_type)
  end

  def registered_effect_type?(_effect_type), do: false

  @spec operation(:echo | :probe, map()) :: {:ok, map()} | {:error, term()}
  def operation(kind, payload) when is_map(payload) do
    case Map.fetch(@operations, kind) do
      {:ok, operation} -> {:ok, Map.put(operation, :payload, payload)}
      :error -> {:error, {:unknown_diagnostic_operation, kind}}
    end
  end

  def operation(kind, _payload), do: {:error, {:invalid_diagnostic_payload, kind}}

  @spec run_echo(map()) :: {:ok, Coordinator.Run.t()} | {:error, term()}
  def run_echo(command) do
    with {:ok, run} <- Coordinator.propose(command),
         {:ok, run} <- Coordinator.authorize(run, authority_decision(command)),
         {:ok, run} <- Coordinator.dispatch(run),
         {:ok, run} <- Coordinator.receive_receipt(run, success_receipt(command)),
         {:ok, run} <- Coordinator.reduce(run),
         {:ok, run} <- Coordinator.project(run) do
      Coordinator.complete(run)
    end
  end

  defp authority_decision(command) do
    %{
      authority_ref: "authority://diagnostic/default",
      decision: :allow,
      tenant_ref: command_value(command, :tenant_ref),
      actor_ref: command_value(command, :actor_ref),
      command_ref: command_value(command, :command_ref),
      trace_ref: command_value(command, :trace_ref),
      decision_hash: "diagnostic-authority",
      boundary_class: "diagnostic",
      posture: "allow_diagnostic"
    }
  end

  defp success_receipt(command) do
    %{
      receipt_ref: "receipt://diagnostic/default",
      effect_ref: command_value(command, :effect_ref),
      status: :success,
      evidence_refs: ["evidence://diagnostic/default"],
      trace_ref: command_value(command, :trace_ref),
      lower_facts: %{"diagnostic" => "echo"}
    }
  end

  defp command_value(command, key),
    do: Map.get(command, key, Map.get(command, Atom.to_string(key)))
end
