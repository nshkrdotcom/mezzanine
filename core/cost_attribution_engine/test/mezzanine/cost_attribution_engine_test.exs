defmodule Mezzanine.CostAttributionEngineTest do
  use ExUnit.Case, async: true

  alias Mezzanine.CostAttributionEngine
  alias OuterBrain.TokenMeter

  test "records append-only cost facts with bounded amount classes" do
    assert {:ok, ledger} = CostAttributionEngine.new_ledger()
    assert {:ok, ledger, fact} = CostAttributionEngine.record(ledger, cost_fact_attrs())

    assert fact.cost_class == :production
    assert fact.amount_class == :redacted_below_floor
    assert length(ledger.facts) == 1

    assert {:ok, ^ledger, duplicate} =
             CostAttributionEngine.record(ledger, cost_fact_attrs())

    assert duplicate.idempotency_key == fact.idempotency_key
  end

  test "rejects unknown cost class and missing token meter ref" do
    attrs = Map.put(cost_fact_attrs(), :cost_class, :unknown)

    assert {:error, {:unknown_cost_fact_enum, :cost_class}} =
             CostAttributionEngine.new_ledger()
             |> elem(1)
             |> CostAttributionEngine.record(attrs)

    attrs = Map.delete(cost_fact_attrs(), :token_meter_ref)

    assert {:error, :missing_token_meter_ref} =
             CostAttributionEngine.new_ledger()
             |> elem(1)
             |> CostAttributionEngine.record(attrs)
  end

  test "denies production cost on replay and eval submissions" do
    attrs = Map.put(cost_fact_attrs(), :replay_mode, :exact)

    assert {:error, :production_cost_for_replay_or_eval_forbidden} =
             CostAttributionEngine.new_ledger()
             |> elem(1)
             |> CostAttributionEngine.record(attrs)
  end

  test "aggregates only bounded keys and denies cross-tenant projections" do
    assert {:ok, ledger} = CostAttributionEngine.new_ledger()
    assert {:ok, ledger, _fact} = CostAttributionEngine.record(ledger, cost_fact_attrs())

    assert {:ok, aggregate} =
             CostAttributionEngine.aggregate(ledger, %{
               tenant_ref: "tenant://a",
               caller_tenant_ref: "tenant://a",
               group_by: :cost_class
             })

    assert aggregate.fact_count == 1
    assert [%{key: :production, fact_count: 1}] = aggregate.groups

    assert {:error, :cross_tenant_cost_aggregation_forbidden} =
             CostAttributionEngine.project(ledger, %{
               tenant_ref: "tenant://a",
               caller_tenant_ref: "tenant://b"
             })
  end

  test "durable postgres adapter is opt-in and rejected until registered" do
    assert {:error, :cost_postgres_adapter_not_registered} =
             CostAttributionEngine.new_ledger(tier: {:durable, :postgres})
  end

  defp cost_fact_attrs do
    assert {:ok, meter_ref} =
             TokenMeter.token_meter_ref(%{
               meter_id: "meter://phase-d",
               provider_family: :codex_cli,
               model_ref: "model://codex/latest",
               tenant_ref: "tenant://a",
               installation_ref: "installation://a",
               revision: 1
             })

    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      run_ref: "run://a",
      connector_instance_ref: "connector://codex",
      provider_account_ref: "provider-account://redacted",
      capability_id: "codex.session.turn",
      operation_class: :provider_effect,
      model_ref: "model://codex/latest",
      persistence_profile_ref: "persistence://memory/default",
      cost_class: :production,
      token_meter_ref: meter_ref,
      amount_class: :redacted_below_floor,
      idempotency_key: "idem-cost-fact",
      trace_id: "trace-cost-fact",
      release_manifest_ref: "release://phase-d"
    }
  end
end
