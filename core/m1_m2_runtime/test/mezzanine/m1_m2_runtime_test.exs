defmodule Mezzanine.M1M2RuntimeTest do
  use ExUnit.Case, async: true

  alias Mezzanine.M1M2Runtime
  alias Mezzanine.M1M2Runtime.WorkflowLowerGateway
  alias Mezzanine.LowerGateway
  alias Mezzanine.M1M2Runtime.WorkflowStartHandoff
  alias Mezzanine.WorkflowRuntime.WorkflowStarterOutbox

  defmodule FakeGovernedIntegrationBridge do
    def invoke_run_intent(%{authorized_invocation_boundary: _boundary} = invocation, opts) do
      invoke_fun = Keyword.fetch!(opts, :invoke_fun)
      capability_id = Keyword.fetch!(opts, :capability_id)
      lower_runtime_kind = Keyword.fetch!(opts, :lower_runtime_kind)

      governed_lower_envelope = %{
        "lower_request_ref" => "lower-request://#{invocation.execution_id}/#{capability_id}",
        "lower_runtime_kind" => Atom.to_string(lower_runtime_kind),
        "capability_id" => capability_id,
        "resource_scope_refs" => Keyword.fetch!(opts, :resource_scope_refs),
        "workspace_root" => Keyword.fetch!(opts, :workspace_root),
        "cwd" => Keyword.fetch!(opts, :cwd),
        "policy_bundle_ref" => Keyword.fetch!(opts, :policy_bundle_ref),
        "script_ref" => Keyword.fetch!(opts, :script_ref),
        "package_refs" => Keyword.fetch!(opts, :package_refs),
        "sandbox_profile_ref" => Keyword.fetch!(opts, :sandbox_profile_ref),
        "attestation_requirement_ref" => Keyword.fetch!(opts, :attestation_requirement_ref)
      }

      input = %{
        governed_lower_envelope: governed_lower_envelope,
        authority: %{
          permission_decision_ref: authority_decision_id(invocation)
        }
      }

      invoke_opts = [governed_lower_envelope: %{capability_id: capability_id}]

      with {:ok, result} <- invoke_fun.(capability_id, input, invoke_opts) do
        {:ok,
         result
         |> Map.put(:governed_lower_envelope, governed_lower_envelope)
         |> Map.put(:governed_lower_receipt, %{
           "lower_receipt_ref" =>
             "lower-receipt://#{governed_lower_envelope["lower_request_ref"]}/succeeded",
           "lower_request_ref" => governed_lower_envelope["lower_request_ref"],
           "lower_runtime_kind" => Atom.to_string(lower_runtime_kind),
           "status" => "succeeded",
           "capability_id" => capability_id,
           "workspace_root" => Keyword.fetch!(opts, :workspace_root),
           "cwd" => Keyword.fetch!(opts, :cwd)
         })}
      end
    end

    defp authority_decision_id(invocation) do
      invocation
      |> Map.fetch!(:invocation_request)
      |> map_value(:authority_packet)
      |> map_value(:decision_id)
    end

    defp map_value(%_{} = struct, key), do: struct |> Map.from_struct() |> map_value(key)

    defp map_value(map, key) when is_map(map),
      do: Map.get(map, key) || Map.get(map, to_string(key))

    defp map_value(_value, _key), do: nil
  end

  test "M1 rejects live providers, connectors, Temporal workers, and credential materializers" do
    assert {:error, {:m1_forbidden_capabilities, forbidden}} =
             M1M2Runtime.admit(%{
               mode: :m1,
               fixture_ref: "fixture://tenant-1/headless/readback",
               projection_ref: "projection://tenant-1/headless/state",
               capabilities: [:fixture_readback, :live_provider, :temporal_worker]
             })

    assert forbidden == [:live_provider, :temporal_worker]
  end

  test "M1 emits deterministic projection receipts without provider authority" do
    assert {:ok, receipt} =
             M1M2Runtime.admit(%{
               mode: :m1,
               fixture_ref: "fixture://tenant-1/headless/readback",
               projection_ref: "projection://tenant-1/headless/state",
               capabilities: [:fixture_readback, :projection_readback]
             })

    assert receipt.mode == :m1
    assert receipt.live_provider_call? == false
    assert receipt.credential_materialized? == false
    assert receipt.temporal_worker_used? == false
  end

  test "M1 rejects durable persistence profiles and substrate capabilities" do
    assert {:error, {:m1_forbidden_capabilities, forbidden}} =
             M1M2Runtime.admit(%{
               mode: :m1,
               fixture_ref: "fixture://tenant-1/headless/readback",
               projection_ref: "projection://tenant-1/headless/state",
               persistence_profile: :integration_postgres,
               capabilities: [:fixture_readback, :postgres_shared, :temporal_durable]
             })

    assert forbidden == [:postgres_shared, :temporal_durable, :integration_postgres]
  end

  test "M2 requires provider, target, lease, policy, and substrate refs" do
    assert {:error, {:m2_missing_required_refs, missing}} =
             valid_m2_attrs()
             |> Map.delete(:credential_lease_ref)
             |> Map.delete(:runtime_substrate_ref)
             |> M1M2Runtime.admit()

    assert missing == [:credential_lease_ref, :runtime_substrate_ref]
  end

  test "M2 fails before mutation without explicit Temporal durable profile and substrate proof" do
    assert {:error, {:m2_missing_required_refs, [:persistence_profile]}} =
             valid_m2_attrs()
             |> Map.delete(:persistence_profile)
             |> M1M2Runtime.admit()

    assert {:error, {:m2_unsupported_persistence_profile, :integration_postgres}} =
             valid_m2_attrs()
             |> Map.put(:persistence_profile, :integration_postgres)
             |> M1M2Runtime.admit()

    assert {:error, {:m2_missing_substrate_capabilities, [:temporal_durable]}} =
             valid_m2_attrs()
             |> Map.put(:capabilities, [])
             |> M1M2Runtime.admit()

    assert {:error,
            {:temporal_substrate_unavailable, %{address: "127.0.0.1:7233", namespace: "default"}}} =
             valid_m2_attrs()
             |> Map.put(:temporal_substrate_available?, false)
             |> M1M2Runtime.admit()
  end

  test "M2 admits live durable execution when all authority refs are present" do
    assert {:ok, receipt} = M1M2Runtime.admit(valid_m2_attrs())

    assert receipt.mode == :m2
    assert receipt.provider_account_ref == "provider-account://tenant-1/claude/main"
    assert receipt.credential_lease_ref == "credential-lease://tenant-1/claude/lease-1"
    assert receipt.runtime_substrate_ref == "temporal://namespace/default/task-queue/agents"
    assert receipt.persistence_profile == :ops_durable
    assert receipt.substrate_preflight_ref == "mezzanine-temporal-dev.service"
    assert receipt.live_provider_call? == true
  end

  test "M2 normalizes string-key substrate preflight attributes" do
    attrs =
      valid_m2_attrs()
      |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)

    assert {:ok, receipt} = M1M2Runtime.admit(attrs)

    assert receipt.persistence_profile == :ops_durable
    assert receipt.substrate_preflight_ref == "mezzanine-temporal-dev.service"
    assert receipt.temporal_worker_used? == true
  end

  test "workflow start handoff builds deterministic outbox rows at the M1/M2 boundary" do
    assert {:ok, row} =
             WorkflowStartHandoff.outbox_row("tenant-1", started_run(), handoff_attrs())

    assert row.workflow_type == "agent_run"
    assert row.workflow_version == "agent-run.v1"
    assert row.resource_ref == "work-object://work-1"
    assert row.idempotency_key == "idem-run-1"
    assert row.trace_id == "trace-run-1"
    assert row.dispatch_state == "queued"
    assert String.starts_with?(row.outbox_id, "workflow-start:")

    assert WorkflowStartHandoff.workflow_start_ref(row) ==
             "workflow-start-outbox://#{row.outbox_id}"

    assert row.workflow_id ==
             WorkflowStarterOutbox.deterministic_workflow_id(%{
               tenant_ref: row.tenant_ref,
               resource_ref: row.resource_ref,
               workflow_type: row.workflow_type,
               command_id: row.command_id,
               release_manifest_ref: row.release_manifest_ref
             })

    assert {:ok, duplicate} =
             WorkflowStartHandoff.outbox_row("tenant-1", started_run(), handoff_attrs())

    assert duplicate.outbox_id == row.outbox_id
    assert duplicate.workflow_id == row.workflow_id
  end

  test "workflow start handoff validates the same local transaction plan without Temporal calls" do
    assert {:ok, row} =
             WorkflowStartHandoff.outbox_row("tenant-1", started_run(), handoff_attrs())

    assert {:ok, plan} = WorkflowStarterOutbox.same_transaction_plan(row)

    assert plan.transaction_boundary == :accepted_command_receipt_and_workflow_start_outbox

    assert Enum.map(plan.operations, & &1.op) == [
             :persist_accepted_command_receipt,
             :insert_workflow_start_outbox_row,
             :insert_oban_dispatch_job
           ]

    assert :temporalex_client_call in plan.forbidden_inside_transaction
  end

  test "explicit workflow lower gateway dispatches through Citadel and governed integration bridge" do
    invoke_fun = fn capability_id, input, opts ->
      send(self(), {:jido_invoke, capability_id, input, opts})

      {:ok,
       %{
         capability_id: capability_id,
         run_id: "jido-run-lower-gateway",
         attempt_id: "jido-attempt-lower-gateway",
         output: %{"status" => "accepted"}
       }}
    end

    claim =
      lower_gateway_claim(%{
        lower_gateway_impl: WorkflowLowerGateway,
        runtime_modules: %{integration_bridge: FakeGovernedIntegrationBridge},
        lower_dispatch_opts: [
          invoke_fun: invoke_fun,
          lower_runtime_kind: :deterministic_fixture,
          runtime_profile_ref: "runtime-profile://sample-app/deterministic",
          runtime_profile_kind: :temporal_local,
          policy_bundle_ref: "policy-bundle://sample-app/default",
          policy_bundle_hash: "sha256:" <> String.duplicate("1", 64),
          cedar_schema_ref: "cedar-schema://sample-app/default",
          cedar_schema_hash: "sha256:" <> String.duplicate("2", 64),
          script_ref: "script://codex/session-turn",
          script_hash: "sha256:" <> String.duplicate("3", 64),
          package_refs: ["package://sample-app/coding-ops"],
          sandbox_profile_ref: "sandbox://local/strict",
          attestation_requirement_ref: "attestation://local/deterministic"
        ]
      })

    assert {:accepted, accepted} = LowerGateway.dispatch(claim)

    assert accepted.owner_repo == :jido_integration
    assert accepted.authority.owner_repo == :citadel
    assert accepted.submission_ref["lower_submission_ref"] == "lower-submission-101"
    assert accepted.submission_ref["run_id"] == "jido-run-lower-gateway"
    assert accepted.lower_receipt["lower_runtime_kind"] == "deterministic_fixture"
    assert accepted.lower_receipt["status"] == "succeeded"
    assert accepted.lower_receipt["capability_id"] == "codex.session.turn"

    assert_received {:jido_invoke, "codex.session.turn", input, opts}
    assert input.governed_lower_envelope["lower_runtime_kind"] == "deterministic_fixture"
    assert input.governed_lower_envelope["workspace_root"] == "/tmp/sample-app/subject-101"
    assert input.governed_lower_envelope["cwd"] == "/tmp/sample-app/subject-101"
    assert input.authority.permission_decision_ref == "decision/execution-101"
    assert Keyword.fetch!(opts, :governed_lower_envelope).capability_id == "codex.session.turn"
  end

  defp valid_m2_attrs do
    %{
      mode: :m2,
      persistence_profile: :ops_durable,
      capabilities: [:temporal_durable],
      temporal_substrate_available?: true,
      substrate_preflight_ref: "mezzanine-temporal-dev.service",
      provider_account_ref: "provider-account://tenant-1/claude/main",
      connector_instance_ref: "connector-instance://tenant-1/claude/default",
      connector_binding_ref: "connector-binding://tenant-1/claude/default",
      credential_lease_ref: "credential-lease://tenant-1/claude/lease-1",
      target_ref: "target://tenant-1/local-process/1",
      attach_grant_ref: "attach-grant://tenant-1/local-process/1",
      operation_policy_ref: "operation-policy://tenant-1/claude/chat",
      runtime_substrate_ref: "temporal://namespace/default/task-queue/agents",
      trace_ref: "trace://tenant-1/m2/1"
    }
  end

  defp started_run do
    %{
      work_object: %{
        id: "work-1",
        program_id: "program-1"
      },
      plan: %{
        id: "plan-1"
      },
      run: %{
        id: "run-1",
        runtime_profile: %{"runtime_profile_ref" => "codex_session"},
        grant_profile: %{"capability_ids" => ["codex.session.turn"]}
      },
      review_unit: nil
    }
  end

  defp handoff_attrs do
    %{
      trace_id: "trace-run-1",
      causation_id: "cause-run-1",
      actor_ref: "actor://ops/lead",
      installation_ref: "installation://sample-app/local",
      idempotency_key: "idem-run-1",
      runtime_profile_ref: "codex_session",
      lower_runtime_kind: "codex_session",
      requested_action_ids: ["codex.session.turn"],
      runtime_policy_config: %{"run" => %{"capability" => "codex.session.turn"}}
    }
  end

  defp lower_gateway_claim(extra_attrs) do
    Map.merge(
      %{
        execution_id: "execution-101",
        tenant_id: "tenant-lower-gateway",
        installation_id: "installation-lower-gateway",
        subject_id: "subject-101",
        trace_id: "trace-lower-gateway",
        causation_id: "cause-lower-gateway",
        submission_dedupe_key: "lower-idem-101",
        compiled_pack_revision: 1,
        binding_snapshot: %{
          "runtime_profile_ref" => "runtime-profile://sample-app/deterministic",
          "runtime_profile_kind" => "temporal_local",
          "lower_runtime_kind" => "deterministic_fixture",
          "resource_scope_refs" => ["workspace://tenant-lower-gateway/subject-101"],
          "workspace_ref" => "workspace://tenant-lower-gateway/subject-101",
          "workspace_root" => "/tmp/sample-app/subject-101"
        },
        dispatch_envelope: %{
          "workflow_id" => "workflow-101",
          "workflow_type" => "execution_attempt",
          "workflow_version" => "execution-attempt.v1",
          "command_id" => "cmd-101",
          "command_receipt_ref" => "command-receipt-101",
          "workflow_input_ref" => "workflow-input-101",
          "lower_submission_ref" => "lower-submission-101",
          "authority_packet_ref" => "authpkt-101",
          "permission_decision_ref" => "decision-101",
          "principal_ref" => "principal-operator",
          "resource_ref" => "resource-work-101",
          "capability" => "codex.session.turn",
          "requested_capability_ids" => ["codex.session.turn"],
          "requested_action_ids" => ["codex.session.turn"],
          "installation_revision" => 4,
          "actor_ref" => "principal-operator",
          "target_id" => "workspace_runtime",
          "service_id" => "workspace_runtime",
          "boundary_class" => "workspace_session",
          "policy_refs" => ["policy-v1"],
          "policy_version" => "policy-v1",
          "policy_epoch" => 1,
          "workspace_mutability" => "read_write",
          "downstream_scope" => "subject:subject-101",
          "execution_intent" => %{
            "prompt" => "Run deterministic lower gateway proof",
            "cwd" => "/tmp/sample-app/subject-101"
          }
        }
      },
      extra_attrs
    )
  end
end
