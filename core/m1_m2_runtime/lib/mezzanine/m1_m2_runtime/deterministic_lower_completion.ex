defmodule Mezzanine.M1M2Runtime.DeterministicLowerCompletion do
  @moduledoc """
  Product-path deterministic lower completion through the governed lower gateway.

  No-live-credential proofs still need the same Citadel, Jido Integration,
  lower-envelope, and execution receipt contracts as live runs. This module
  dispatches through `WorkflowLowerGateway`, then records the terminal receipt
  onto the current execution row so northbound readback does not fabricate
  product-local pending lower receipts.
  """

  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.LowerGateway
  alias Mezzanine.M1M2Runtime.WorkflowLowerGateway
  alias Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow
  alias Mezzanine.WorkflowRuntime.TerminalLowerReceiptShape

  @release_manifest_ref "phase6-deterministic-lower-completion"
  @default_integration_bridge :"Elixir.Mezzanine.IntegrationBridge"
  @default_jido_lane :"Elixir.Jido.Integration.V2.DeterministicLowerLane"

  @spec complete(String.t(), map(), map(), map(), map() | keyword(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def complete(tenant_id, started_run, workflow_handoff, execution_handoff, attrs, opts \\ [])
      when is_binary(tenant_id) and is_map(started_run) and is_map(workflow_handoff) and
             is_map(execution_handoff) and is_list(opts) do
    attrs = normalize_map(attrs)

    with {:ok, %ExecutionRecord{} = execution} <- fetch_execution(execution_handoff) do
      if terminal_lower_receipt?(execution.lower_receipt) do
        {:ok, Map.put(execution_handoff, :deterministic_lower, %{status: :already_recorded})}
      else
        complete_execution(
          tenant_id,
          started_run,
          workflow_handoff,
          execution_handoff,
          attrs,
          opts
        )
      end
    end
  end

  defp complete_execution(
         tenant_id,
         started_run,
         workflow_handoff,
         execution_handoff,
         attrs,
         opts
       ) do
    execution = Map.fetch!(execution_handoff, :execution)
    claim = dispatch_claim(tenant_id, started_run, workflow_handoff, execution, attrs, opts)

    with {:accepted, accepted} <- LowerGateway.dispatch(claim),
         {:ok, facts} <- deterministic_facts(accepted),
         lower_receipt <- lower_receipt_payload(execution, accepted, facts, attrs),
         {:ok, updated_execution} <- record_completed(execution, lower_receipt, attrs),
         {:ok, persisted} <-
           persist_terminal_receipt(execution, accepted, facts, lower_receipt, attrs) do
      {:ok,
       execution_handoff
       |> Map.put(:execution, updated_execution)
       |> Map.put(:status, :completed)
       |> Map.put(:deterministic_lower, %{
         status: :recorded,
         lower_receipt_ref: lower_receipt["lower_receipt_ref"],
         receipt: persisted
       })}
    else
      {:rejected, reason} -> {:error, {:deterministic_lower_rejected, reason}}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_deterministic_lower_result, other}}
    end
  end

  @spec dispatch_claim(String.t(), map(), map(), struct(), map(), keyword()) ::
          Mezzanine.LowerGateway.dispatch_claim()
  defp dispatch_claim(tenant_id, started_run, workflow_handoff, execution, attrs, opts) do
    binding = normalize_map(execution.binding_snapshot || %{})
    dispatch = normalize_map(execution.dispatch_envelope || %{})
    run = normalize_map(Map.get(started_run, :run, %{}))
    execution_id = required_text(execution.id, "execution://deterministic")

    installation_id =
      required_text(execution.installation_id, "installation://#{tenant_id}/default")

    subject_id = required_text(execution.subject_id, "subject://#{execution_id}")
    trace_id = required_text(execution.trace_id, "trace://#{execution_id}")

    submission_dedupe_key =
      required_text(execution.submission_dedupe_key, "dedupe://#{execution_id}")

    runtime_profile =
      normalize_map(Map.get(binding, "runtime_profile") || Map.get(run, "runtime_profile") || %{})

    capability_id = capability_id(binding, dispatch, runtime_profile)
    workflow_id = string_value(dispatch, "workflow_id") || "workflow://#{execution_id}"
    lower_request_ref = "lower-request://#{execution_id}/#{URI.encode_www_form(capability_id)}"
    actor = actor_ref_string(attrs)

    lower_context = %{
      execution_id: execution_id,
      subject_id: subject_id,
      submission_dedupe_key: submission_dedupe_key
    }

    %{
      execution_id: execution_id,
      tenant_id: tenant_id,
      installation_id: installation_id,
      subject_id: subject_id,
      trace_id: trace_id,
      causation_id: optional_text(execution.causation_id),
      submission_dedupe_key: submission_dedupe_key,
      compiled_pack_revision: integer_value(execution.compiled_pack_revision, 1),
      binding_snapshot: binding,
      dispatch_envelope:
        dispatch
        |> Map.put_new("workflow_id", workflow_id)
        |> Map.put_new("workflow_type", "execution_attempt")
        |> Map.put_new("workflow_version", "execution-attempt.v1")
        |> Map.put_new("command_id", "execution:#{execution_id}")
        |> Map.put_new(
          "command_receipt_ref",
          "command-receipt://mezzanine/execution/#{execution_id}"
        )
        |> Map.put_new("workflow_input_ref", "workflow-input://#{execution_id}")
        |> Map.put_new(
          "lower_submission_ref",
          "lower-submission://#{submission_dedupe_key}"
        )
        |> Map.put_new("authority_packet_ref", "authority-packet://#{execution_id}")
        |> Map.put_new("permission_decision_ref", "permission-decision://#{execution_id}")
        |> Map.put_new("workflow_start_ref", map_value(workflow_handoff, :workflow_start_ref))
        |> Map.put_new("principal_ref", actor)
        |> Map.put_new("actor_ref", actor)
        |> Map.put_new("resource_ref", "work-object://#{subject_id}")
        |> Map.put_new("capability", capability_id)
        |> Map.put_new("requested_capability_ids", [capability_id, "linear.comments.update"])
        |> Map.put_new("requested_action_ids", [capability_id])
        |> Map.put_new("installation_revision", 1)
        |> Map.put_new("workspace_mutability", "read_write")
        |> Map.put_new("downstream_scope", "subject:#{subject_id}")
        |> Map.put_new("review_required", review_required?(execution)),
      actor_ref: actor,
      principal_ref: actor,
      lower_gateway_impl: WorkflowLowerGateway,
      runtime_modules: %{
        integration_bridge: Keyword.get(opts, :integration_bridge, @default_integration_bridge)
      },
      lower_dispatch_opts:
        lower_dispatch_opts(
          execution,
          binding,
          dispatch,
          runtime_profile,
          capability_id,
          lower_request_ref,
          lower_context,
          opts
        )
    }
  end

  defp lower_dispatch_opts(
         execution,
         binding,
         dispatch,
         runtime_profile,
         capability_id,
         lower_request_ref,
         lower_context,
         opts
       ) do
    encoded = URI.encode_www_form(lower_request_ref)
    execution_id = Map.fetch!(lower_context, :execution_id)
    subject_id = Map.fetch!(lower_context, :subject_id)
    submission_dedupe_key = Map.fetch!(lower_context, :submission_dedupe_key)

    refs = lower_dispatch_refs(binding, dispatch)

    [
      invoke_fun: Keyword.get(opts, :invoke_fun, deterministic_invoke_fun()),
      capability_id: capability_id,
      action_id: capability_id,
      lower_request_ref: lower_request_ref,
      lower_runtime_kind: lower_runtime_kind(binding, dispatch, runtime_profile),
      runtime_profile_ref: runtime_profile_ref(execution, binding, dispatch, runtime_profile),
      runtime_profile_kind: runtime_profile_kind(binding, dispatch, runtime_profile),
      run_ref: string_value(binding, "run_id") || execution.id,
      workflow_ref: string_value(dispatch, "workflow_id") || "workflow://#{execution.id}",
      connector_ref: "jido/connectors/codex_cli",
      connector_manifest_ref: "manifest://jido/connectors/codex_cli@deterministic",
      connector_manifest_hash:
        sha256_ref("jido/connectors/codex_cli:#{capability_id}:deterministic"),
      connector_manifest_state: :active,
      capability_negotiation_ref: "cap-neg://#{encoded}/#{capability_id}",
      policy_bundle_ref: refs.policy_bundle_ref,
      policy_bundle_hash: sha256_ref(refs.policy_bundle_ref),
      cedar_schema_ref: refs.cedar_schema_ref,
      cedar_schema_hash: sha256_ref(refs.cedar_schema_ref),
      script_ref: "script://codex/session-turn/deterministic",
      script_hash: sha256_ref("script://codex/session-turn/deterministic"),
      script_api_version: "v1",
      declared_actions: [capability_id, "linear.comments.update", "github.pr.evidence"],
      package_refs: package_refs(binding, dispatch),
      resource_scope_refs: resource_scope_refs(subject_id, binding, dispatch),
      workspace_ref: workspace_ref(subject_id, binding, dispatch),
      target_ref:
        string_value(dispatch, "target_ref") ||
          "target://workspace-runtime/#{subject_id}",
      sandbox_profile_ref: refs.sandbox_profile_ref,
      sandbox_level: :strict,
      acceptable_attestation: ["local-erlexec-weak"],
      attestation_requirement_ref: "local-erlexec-weak",
      evidence_profile_ref: refs.evidence_profile_ref,
      redaction_profile_ref: refs.redaction_profile_ref,
      input_ref: "input://#{encoded}",
      input_hash: sha256_ref("#{execution_id}:#{submission_dedupe_key}:#{capability_id}")
    ]
  end

  defp record_completed(%ExecutionRecord{} = execution, lower_receipt, attrs) do
    ExecutionRecord.record_completed(execution, %{
      receipt_id: lower_receipt["receipt_id"],
      lower_receipt: lower_receipt,
      normalized_outcome: %{
        "status" => "succeeded",
        "lower_receipt_ref" => lower_receipt["lower_receipt_ref"]
      },
      artifact_refs: artifact_ref_strings(lower_receipt),
      trace_id: execution.trace_id,
      causation_id: execution.causation_id || lower_receipt["lower_receipt_ref"],
      actor_ref: actor_ref(attrs)
    })
  end

  defp persist_terminal_receipt(execution, accepted, facts, lower_receipt, attrs) do
    dispatch = normalize_map(execution.dispatch_envelope || %{})

    routing_facts =
      accepted
      |> map_value(:routing_facts, %{})
      |> normalize_map()
      |> Map.merge(facts)
      |> Map.merge(%{
        "required_evidence" => ["github_pr", "codex_session", "source_workpad"],
        "review_required" => review_required?(execution),
        "actor_ref" => actor_ref(attrs),
        "artifact_refs" => lower_receipt["artifact_refs"],
        "evidence_artifact_refs" => lower_receipt["artifact_refs"]
      })

    ExecutionLifecycleWorkflow.persist_terminal_receipt_activity(%{
      workflow_id: string_value(dispatch, "workflow_id") || "workflow://#{execution.id}",
      terminal_state: "succeeded",
      terminal_event_ref: lower_receipt["lower_event_ref"] || lower_receipt["lower_receipt_ref"],
      lower_receipt_ref: lower_receipt["lower_receipt_ref"],
      trace_id: execution.trace_id,
      release_manifest_ref: @release_manifest_ref,
      installation_ref: execution.installation_id,
      subject_ref: execution.subject_id,
      execution_id: execution.id,
      causation_id: execution.causation_id,
      correlation_id: execution.causation_id || lower_receipt["lower_receipt_ref"],
      idempotency_key: execution.submission_dedupe_key,
      actor_ref: actor_ref(attrs),
      signal_id: lower_receipt["receipt_id"],
      receipt_state: "succeeded",
      lower_run_ref: lower_receipt["run_id"],
      lower_attempt_ref: lower_receipt["attempt_id"],
      lower_event_ref: lower_receipt["lower_event_ref"],
      routing_facts: routing_facts
    })
  end

  defp lower_receipt_payload(execution, accepted, facts, attrs) do
    TerminalLowerReceiptShape.from_deterministic_completion(execution, accepted, facts, attrs)
  end

  defp deterministic_facts(accepted) do
    accepted
    |> map_value(:provider_submission, %{})
    |> map_value(:output, %{})
    |> map_value(:deterministic_lower)
    |> case do
      %{} = facts -> {:ok, normalize_map(facts)}
      _other -> {:error, :missing_deterministic_lower_facts}
    end
  end

  defp deterministic_invoke_fun do
    fn capability_id, input, opts ->
      if Code.ensure_loaded?(@default_jido_lane) and
           function_exported?(@default_jido_lane, :invoke, 3) do
        :erlang.apply(@default_jido_lane, :invoke, [capability_id, input, opts])
      else
        {:error, {:module_unavailable, @default_jido_lane, :invoke, 3}}
      end
    end
  end

  defp fetch_execution(%{execution: %ExecutionRecord{} = execution}), do: {:ok, execution}
  defp fetch_execution(_handoff), do: {:error, :missing_execution_handoff}

  defp terminal_lower_receipt?(receipt) when is_map(receipt) and map_size(receipt) > 0 do
    ref = string_value(receipt, "lower_receipt_ref") || string_value(receipt, "receipt_ref")
    state = string_value(receipt, "receipt_state") || string_value(receipt, "state")

    is_binary(ref) and not String.contains?(ref, "/pending/") and
      state in ["succeeded", "completed", "success"]
  end

  defp terminal_lower_receipt?(_receipt), do: false

  defp capability_id(binding, dispatch, runtime_profile) do
    string_value(dispatch, "capability") ||
      first_string(list_value(dispatch, "requested_capability_ids")) ||
      first_string(list_value(binding, "requested_capability_ids")) ||
      string_value(runtime_profile, "capability_id") ||
      "codex.session.turn"
  end

  defp lower_runtime_kind(binding, dispatch, runtime_profile) do
    case string_value(dispatch, "lower_runtime_kind") ||
           string_value(binding, "lower_runtime_kind") ||
           string_value(runtime_profile, "lower_runtime_kind") do
      nil -> :codex_session
      "codex_session" -> :codex_session
      value -> value
    end
  end

  defp runtime_profile_ref(execution, binding, dispatch, runtime_profile) do
    string_value(dispatch, "runtime_profile_ref") ||
      string_value(binding, "runtime_profile_ref") ||
      string_value(runtime_profile, "runtime_profile_ref") ||
      "runtime-profile://deterministic/#{execution.installation_id}"
  end

  defp runtime_profile_kind(binding, dispatch, runtime_profile) do
    case string_value(dispatch, "runtime_profile_kind") ||
           string_value(binding, "runtime_profile_kind") ||
           string_value(runtime_profile, "runtime_profile_kind") do
      nil -> :temporal_local
      "temporal_local" -> :temporal_local
      value -> value
    end
  end

  defp lower_dispatch_refs(binding, dispatch) do
    %{
      policy_bundle_ref:
        lower_ref(dispatch, binding, "policy_bundle_ref", "policy-bundle://runtime/deterministic"),
      cedar_schema_ref:
        lower_ref(dispatch, binding, "cedar_schema_ref", "cedar-schema://runtime/deterministic"),
      sandbox_profile_ref:
        lower_ref(dispatch, binding, "sandbox_profile_ref", "sandbox://runtime/strict"),
      evidence_profile_ref:
        lower_ref(
          dispatch,
          binding,
          "evidence_profile_ref",
          "evidence://runtime/github-pr-plus-workpad"
        ),
      redaction_profile_ref:
        lower_ref(dispatch, binding, "redaction_profile_ref", "redaction://runtime/default")
    }
  end

  defp lower_ref(dispatch, binding, key, default) do
    string_value(dispatch, key) || string_value(binding, key) || default
  end

  defp package_refs(binding, dispatch) do
    first_non_empty_list([
      list_value(dispatch, "package_refs"),
      list_value(binding, "package_refs"),
      ["package://runtime/default-agent"]
    ])
  end

  defp resource_scope_refs(subject_id, binding, dispatch) do
    first_non_empty_list([
      list_value(dispatch, "resource_scope_refs"),
      list_value(binding, "resource_scope_refs"),
      ["workspace://work_object/#{subject_id}", "source_binding://linear_primary"]
    ])
  end

  defp workspace_ref(subject_id, binding, dispatch) do
    string_value(dispatch, "workspace_ref") ||
      string_value(binding, "workspace_ref") ||
      "workspace://work_object/#{subject_id}"
  end

  defp review_required?(%ExecutionRecord{} = execution) do
    execution.intent_snapshot
    |> normalize_map()
    |> map_value(:review_required)
    |> truthy?()
  end

  defp actor_ref(attrs) do
    case map_value(attrs, :actor_ref) do
      %{} = actor -> normalize_map(actor)
      value when is_binary(value) and value != "" -> %{"kind" => "human", "ref" => value}
      _other -> %{"kind" => "system", "ref" => "deterministic_lower_completion"}
    end
  end

  defp actor_ref_string(attrs) do
    case actor_ref(attrs) do
      %{"ref" => ref} when is_binary(ref) -> ref
      _other -> "system://deterministic_lower_completion"
    end
  end

  defp artifact_ref_strings(lower_receipt) do
    lower_receipt
    |> map_value(:artifact_refs, [])
    |> List.wrap()
    |> Enum.flat_map(fn
      %{} = ref -> ref |> map_value(:content_ref) |> List.wrap()
      ref when is_binary(ref) -> [ref]
      _other -> []
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp list_value(map, key) do
    case map_value(map, key) do
      nil -> []
      value when is_list(value) -> value
      value -> [value]
    end
  end

  defp first_string(values), do: Enum.find(values, &is_binary/1)

  defp first_non_empty_list(lists), do: Enum.find(lists, &(&1 != [])) || []

  defp map_value(nil, _key, default), do: default
  defp map_value(map, key, default), do: map_value(map, key) || default

  defp map_value(nil, _key), do: nil
  defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)

  defp map_value(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || Map.get(map, to_string(key))
  end

  defp map_value(map, key) when is_map(map) and is_binary(key) do
    Map.get(map, key) || map_existing_atom_value(map, key)
  end

  defp map_value(_value, _key), do: nil

  defp map_existing_atom_value(map, key) do
    Map.get(map, String.to_existing_atom(key))
  rescue
    ArgumentError -> nil
  end

  defp string_value(map, key) do
    case map_value(map, key) do
      nil -> nil
      value when is_binary(value) and value in ["", "nil"] -> nil
      value when is_binary(value) and value != "" -> value
      value when is_atom(value) -> Atom.to_string(value)
      value when is_integer(value) -> Integer.to_string(value)
      _other -> nil
    end
  end

  @spec required_text(term(), String.t()) :: String.t()
  defp required_text(value, _fallback) when is_binary(value) and value not in ["", "nil"],
    do: value

  defp required_text(value, _fallback) when is_integer(value), do: Integer.to_string(value)
  defp required_text(_value, fallback), do: fallback

  @spec optional_text(term()) :: String.t() | nil
  defp optional_text(value) when is_binary(value) and value not in ["", "nil"], do: value
  defp optional_text(value) when is_integer(value), do: Integer.to_string(value)
  defp optional_text(_value), do: nil

  @spec integer_value(term(), integer()) :: integer()
  defp integer_value(value, _fallback) when is_integer(value), do: value

  defp integer_value(value, fallback) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _other -> fallback
    end
  end

  defp integer_value(_value, fallback), do: fallback

  defp normalize_map(%_{} = struct), do: struct |> Map.from_struct() |> normalize_map()

  defp normalize_map(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), normalize_value(value)} end)
  end

  defp normalize_map(_value), do: %{}

  defp normalize_value(%_{} = struct), do: struct |> Map.from_struct() |> normalize_value()
  defp normalize_value(map) when is_map(map), do: normalize_map(map)
  defp normalize_value(values) when is_list(values), do: Enum.map(values, &normalize_value/1)
  defp normalize_value(nil), do: nil
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp truthy?(value), do: value in [true, "true", 1, "1"]

  defp sha256_ref(value) do
    "sha256:" <> (:crypto.hash(:sha256, to_string(value)) |> Base.encode16(case: :lower))
  end
end
