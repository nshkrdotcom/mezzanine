defmodule Mezzanine.ConfigRegistry.PolicyRegistryTest do
  use Mezzanine.ConfigRegistry.DataCase, async: false

  alias Mezzanine.ConfigRegistry.{
    ClusterInvalidation,
    Policy,
    PolicyRegistry,
    ReadPolicy,
    TransformPolicy
  }

  @effective_from ~U[2026-04-23 00:00:00Z]
  @effective_until ~U[2026-04-24 00:00:00Z]

  test "registers versioned policies and resolves by granularity precedence" do
    telemetry_id = attach_invalidation_telemetry()

    assert {:ok, %Policy{} = global} =
             read_policy("read-global", :global)
             |> register_policy()

    assert {:ok, %Policy{} = tenant} =
             read_policy("read-tenant", :tenant)
             |> register_policy(tenant_ref: "tenant-a")

    assert {:ok, %Policy{} = installation} =
             read_policy("read-installation", :installation)
             |> register_policy(tenant_ref: "tenant-a", installation_ref: "installation-a")

    assert global.version == 1
    assert tenant.granularity_scope == :tenant

    assert {:ok, %Policy{} = resolved} =
             PolicyRegistry.resolve(
               :read,
               %{
                 tenant_ref: "tenant-a",
                 installation_ref: "installation-a"
               },
               at: ~U[2026-04-23 12:00:00Z]
             )

    assert resolved.policy_id == installation.policy_id
    assert resolved.granularity_scope == :installation

    assert_receive {:cluster_invalidation, %{message: message}}
    assert message.topic =~ "memory.policy."
    assert message.source_node_ref == "node://mez_1@127.0.0.1/node-a"
    assert is_binary(message.commit_lsn)
    assert message.commit_hlc == commit_hlc()

    expected_topic =
      ClusterInvalidation.policy_topic!(
        tenant_ref: "tenant-a",
        installation_ref: "installation-a",
        kind: :read,
        policy_id: "read-installation",
        version: 1
      )

    assert_receive {:cluster_invalidation, %{message: %{topic: ^expected_topic}}}

    :telemetry.detach(telemetry_id)
  end

  test "expired policies do not resolve" do
    assert {:ok, %Policy{}} =
             read_policy("read-expired", :tenant)
             |> register_policy(
               tenant_ref: "tenant-a",
               effective_from: ~U[2026-04-20 00:00:00Z],
               effective_until: ~U[2026-04-21 00:00:00Z]
             )

    assert {:error, :not_found} =
             PolicyRegistry.resolve(:read, %{tenant_ref: "tenant-a"},
               at: ~U[2026-04-23 12:00:00Z]
             )
  end

  test "rejects conflicting active policy versions at the same scope" do
    assert {:ok, %Policy{}} =
             read_policy("read-conflict", :tenant)
             |> register_policy(tenant_ref: "tenant-a")

    assert {:error, {:conflicting_policy_precedence, details}} =
             read_policy("read-conflict", :tenant)
             |> register_policy(tenant_ref: "tenant-a")

    assert details.policy_id == "read-conflict"
    assert details.granularity_scope == :tenant
  end

  test "rejects policy registration without invalidation evidence before write" do
    assert {:error, %ArgumentError{} = error} =
             read_policy("read-no-evidence", :tenant)
             |> PolicyRegistry.register(
               tenant_ref: "tenant-a",
               effective_from: @effective_from,
               effective_until: @effective_until
             )

    assert error.message =~ "cluster_invalidation.source_node_ref"

    assert {:error, :not_found} =
             PolicyRegistry.resolve(:read, %{tenant_ref: "tenant-a"},
               at: ~U[2026-04-23 12:00:00Z]
             )
  end

  test "rolls back policy registration when invalidation publish fails" do
    previous_publisher =
      Application.get_env(:mezzanine_config_registry, :cluster_invalidation_publisher)

    Application.put_env(
      :mezzanine_config_registry,
      :cluster_invalidation_publisher,
      {__MODULE__, :reject_invalidation}
    )

    on_exit(fn ->
      if is_nil(previous_publisher) do
        Application.delete_env(:mezzanine_config_registry, :cluster_invalidation_publisher)
      else
        Application.put_env(
          :mezzanine_config_registry,
          :cluster_invalidation_publisher,
          previous_publisher
        )
      end
    end)

    assert {:error, :publish_failed} =
             read_policy("read-publish-fails", :tenant)
             |> register_policy(tenant_ref: "tenant-a")

    assert {:error, :not_found} =
             PolicyRegistry.resolve(:read, %{tenant_ref: "tenant-a"},
               at: ~U[2026-04-23 12:00:00Z]
             )
  end

  test "stores transform specs with deterministic and stochastic provenance" do
    assert {:ok, %TransformPolicy{} = deterministic} =
             TransformPolicy.new(%{
               policy_id: "transform-redact",
               version: 1,
               granularity_scope: :tenant,
               pipeline: [%{kind: :redact, patterns: ["secret"]}],
               determinism: :deterministic,
               output_hash_anchor:
                 "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
               access_projection_rule: %{mode: "same_access"}
             })

    assert {:ok, %Policy{} = record} =
             PolicyRegistry.register(deterministic,
               tenant_ref: "tenant-a",
               effective_from: @effective_from,
               effective_until: @effective_until,
               source_node_ref: "node://mez_1@127.0.0.1/node-a",
               commit_hlc: commit_hlc()
             )

    assert record.kind == :transform
    assert record.spec.determinism == :deterministic
    assert [%{kind: :redact}] = record.spec.pipeline
  end

  defp read_policy(policy_id, granularity_scope) do
    attrs = %{
      policy_id: policy_id,
      version: 1,
      granularity_scope: granularity_scope,
      candidate_filter: %{tiers: [:private, :shared, :governed]},
      ranking_fn: "recency_then_similarity",
      top_k_private: 5,
      top_k_shared: 3,
      top_k_governed: 1,
      transform_ref: "transform://identity",
      degraded_behavior: :fail_empty,
      audit_level: :standard
    }

    assert {:ok, %ReadPolicy{} = policy} = ReadPolicy.new(attrs)
    policy
  end

  defp register_policy(policy, opts \\ []) do
    defaults = [
      effective_from: @effective_from,
      effective_until: @effective_until,
      source_node_ref: "node://mez_1@127.0.0.1/node-a",
      commit_hlc: commit_hlc()
    ]

    PolicyRegistry.register(policy, Keyword.merge(defaults, opts))
  end

  defp attach_invalidation_telemetry do
    handler_id = {__MODULE__, self(), make_ref()}

    :telemetry.attach(
      handler_id,
      [:mezzanine, :cluster_invalidation, :publish],
      fn _event, _measurements, metadata, _config ->
        send(self(), {:cluster_invalidation, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    handler_id
  end

  defp commit_hlc do
    %{
      "w" => 1_776_947_200_000_000_000,
      "l" => 0,
      "n" => "node://mez_1@127.0.0.1/node-a"
    }
  end

  def reject_invalidation(_message), do: {:error, :publish_failed}
end
