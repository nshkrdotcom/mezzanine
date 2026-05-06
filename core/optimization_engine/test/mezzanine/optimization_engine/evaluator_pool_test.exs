defmodule Mezzanine.OptimizationEngine.EvaluatorPoolTest do
  use ExUnit.Case, async: true

  alias Mezzanine.OptimizationEngine.{EvaluatorPool, RunSpec}

  test "plans leased idempotent batches and dedupes normalized results" do
    spec = RunSpec.new!(base_spec())

    assert {:ok, pool} =
             EvaluatorPool.plan(spec,
               candidate_refs: ["candidate:component:instruction:v1"],
               example_refs: ["example:1", "example:2"],
               worker_ref: "worker:evaluator:1"
             )

    assert [%EvaluatorPool.Batch{} = batch] = pool.batches
    assert batch.run_ref == "optimization_run:phase7"
    assert batch.candidate_ref == "candidate:component:instruction:v1"
    assert batch.lease.owner_ref == "worker:evaluator:1"
    assert batch.lease.expires_at_ref == "lease_expiry:optimization_run:phase7:batch:1"

    assert batch.idempotency_key ==
             "idempotency:optimization_run:phase7:candidate:component:instruction:v1:batch:1"

    assert batch.retry_policy_ref == "retry:optimization:bounded"
    assert batch.cost_refs == ["cost:optimization:batch:1"]
    assert batch.trace_refs == ["trace:optimization:batch:1"]

    result_attrs = %{
      batch_ref: batch.batch_ref,
      idempotency_key: batch.idempotency_key,
      eval_ref: "eval:batch:1",
      score: 1.0,
      cost_refs: ["cost:optimization:batch:1"],
      trace_refs: ["trace:optimization:batch:1"]
    }

    assert {:ok, pool, result} = EvaluatorPool.record_result(pool, result_attrs)
    refute result.deduped?

    assert {:ok, _pool, duplicate} = EvaluatorPool.record_result(pool, result_attrs)
    assert duplicate.deduped?
    assert duplicate.eval_ref == "eval:batch:1"
  end

  defp base_spec do
    %{
      run_ref: "optimization_run:phase7",
      tenant_ref: "tenant:phase7",
      authority_ref: "authority:phase7",
      target_ref: "target:gepa:instruction",
      framework_run_ref: "run:gepa:phase7",
      checkpoint_ref: "checkpoint:memory:gepa",
      budget_ref: "budget:optimization:phase7",
      eval_suite_ref: "eval_suite:phase7",
      replay_bundle_ref: "replay:phase7",
      trace_ref: "trace:optimization:phase7"
    }
  end
end
