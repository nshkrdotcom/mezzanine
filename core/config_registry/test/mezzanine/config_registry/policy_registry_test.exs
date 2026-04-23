defmodule Mezzanine.ConfigRegistry.PolicyRegistryTest do
  use Mezzanine.ConfigRegistry.DataCase, async: false

  alias Mezzanine.ConfigRegistry.{Policy, PolicyRegistry, ReadPolicy, TransformPolicy}

  @effective_from ~U[2026-04-23 00:00:00Z]
  @effective_until ~U[2026-04-24 00:00:00Z]

  test "registers versioned policies and resolves by granularity precedence" do
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
               effective_until: @effective_until
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
      effective_until: @effective_until
    ]

    PolicyRegistry.register(policy, Keyword.merge(defaults, opts))
  end
end
