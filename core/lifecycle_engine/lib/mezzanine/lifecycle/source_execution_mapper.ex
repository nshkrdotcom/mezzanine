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

  @spec to_execution_attempt_input(map() | struct(), keyword()) ::
          {:ok, WorkflowExecutionLifecycleInput.t()} | {:error, term()}
  def to_execution_attempt_input(source, opts \\ []) when is_map(source) and is_list(opts) do
    with :ok <- validate_required_source(source),
         attrs <- execution_attempt_attrs(source, opts) do
      WorkflowExecutionLifecycleInput.new(attrs)
    end
  rescue
    error in ArgumentError -> {:error, error}
  end

  @spec to_lifecycle_advance(map() | struct(), keyword()) :: {:ok, map()} | {:error, term()}
  def to_lifecycle_advance(source, opts \\ []) when is_map(source) and is_list(opts) do
    with :ok <- validate_required_source(source) do
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
    workflow_id = value(source, :workflow_id) || deterministic_workflow_id(source, idempotency_key)
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
    case Enum.find(@required_source_fields, &(blank?(value(source, &1)))) do
      nil -> :ok
      field -> {:error, {:missing_required_source_field, field}}
    end
  end

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
    digest = :crypto.hash(:sha256, idempotency_key) |> Base.encode16(case: :lower) |> binary_part(0, 24)
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
        raise ArgumentError, "source field #{inspect(key)} must be a non-empty string, got: #{inspect(value)}"
    end
  end

  defp required_non_neg_integer!(source, key) do
    case required!(source, key) do
      value when is_integer(value) and value >= 0 -> value
      value -> raise ArgumentError, "source field #{inspect(key)} must be a non-negative integer, got: #{inspect(value)}"
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
end
