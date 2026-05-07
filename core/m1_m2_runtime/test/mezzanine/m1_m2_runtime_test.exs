defmodule Mezzanine.M1M2RuntimeTest do
  use ExUnit.Case, async: true

  alias Mezzanine.M1M2Runtime

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
end
