defmodule Mezzanine.EvalEngineTest do
  use ExUnit.Case, async: true

  alias Mezzanine.EvalEngine

  test "runs deterministic eval cases and composes worst verdict" do
    assert {:ok, run} =
             EvalEngine.run(
               suite_attrs([
                 case_attrs("case-2", %{tool: "call"}, %{tool: "other"}),
                 case_attrs("case-1", %{tool: "call"}, %{tool: "call"})
               ]),
               variant_config(),
               max_concurrency: 2,
               parent_budget_units: 10
             )

    assert run.verdict == :regress
    assert run.cost_class == :eval
    assert Enum.map(run.case_projections, & &1.case_ref) == ["case-1", "case-2"]
  end

  test "rejects unauthorized, raw, missing, and unbounded eval inputs" do
    assert {:error, :unauthorized_eval_run} =
             EvalEngine.run(suite_attrs([case_attrs("case-1")]), variant_config(),
               authorized_tenants: ["tenant://other"]
             )

    assert {:error, {:raw_eval_payload_forbidden, :expected_output}} =
             suite_attrs([Map.put(case_attrs("case-1"), :expected_output, "raw")])
             |> EvalEngine.suite_ref()

    assert {:error, :eval_suite_has_no_cases} =
             suite_attrs([])
             |> EvalEngine.suite_ref()

    assert {:error, :eval_concurrency_unbounded} =
             EvalEngine.run(suite_attrs([case_attrs("case-1")]), variant_config(),
               max_concurrency: 100
             )

    assert {:error, :eval_parent_budget_exceeded} =
             EvalEngine.run(
               suite_attrs([Map.put(case_attrs("case-1"), :budget_units, 20)]),
               variant_config(),
               parent_budget_units: 1
             )
  end

  defp suite_attrs(cases) do
    %{
      suite_ref: "eval-suite://phase-c",
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-eval",
      trace_ref: "trace://eval",
      regression_oracle: :exact_shape,
      cases: cases,
      release_manifest_ref: "release://phase-c"
    }
  end

  defp case_attrs(case_ref, expected_shape \\ %{value: "ok"}, observed_shape \\ %{value: "ok"}) do
    %{
      case_ref: case_ref,
      input_prompt_ref: "prompt://phase-c",
      expected_output_ref: "eval-output://#{case_ref}",
      expected_shape: expected_shape,
      observed_shape: observed_shape,
      evidence_ref: "eval-evidence://#{case_ref}",
      budget_units: 1
    }
  end

  defp variant_config do
    %{
      prompt_revision: 1,
      model_ref: "model://deterministic",
      policy_revision: "policy://phase-c",
      guard_chain_ref: "guard-chain://phase-c",
      memory_profile_ref: "memory-profile://fixture"
    }
  end
end
