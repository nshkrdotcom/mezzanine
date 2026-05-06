defmodule Mezzanine.Audit.AIPlatformFactTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Audit.AIPlatformFact

  test "memory access audit facts carry bounded refs and no raw bodies" do
    assert {:ok, fact} = AIPlatformFact.memory_access_recorded(memory_attrs())

    assert fact.fact_kind == :memory_access_recorded
    assert fact.installation_id == "installation://a"
    assert fact.trace_id == "trace://a"
    assert fact.payload["operation"] == "write"
    refute Map.has_key?(fact.payload, "body")
    refute Map.has_key?(fact.payload, "raw_body")

    assert {:error, {:raw_ai_platform_audit_payload_forbidden, :memory_body}} =
             memory_attrs()
             |> Map.put(:memory_body, "raw memory")
             |> AIPlatformFact.memory_access_recorded()
  end

  test "budget enforced audit facts carry bounded accounting fields" do
    assert {:ok, fact} = AIPlatformFact.budget_enforced(budget_attrs())

    assert fact.fact_kind == :budget_enforced
    assert fact.payload["locus"] == "preflight"
    assert fact.payload["decision_class"] == "deny_exhausted"
    assert fact.payload["requested_units"] == 10

    assert {:error, {:invalid_ai_platform_audit_field, :residual_units}} =
             budget_attrs()
             |> Map.put(:residual_units, -1)
             |> AIPlatformFact.budget_enforced()
  end

  test "prompt resolved audit facts carry prompt refs without raw bodies" do
    assert {:ok, fact} = AIPlatformFact.prompt_resolved(prompt_attrs())

    assert fact.fact_kind == :prompt_resolved
    assert fact.payload["prompt_id"] == "prompt://a"
    assert fact.payload["revision"] == 2
    assert fact.payload["decision_class"] == "resolved"

    assert {:error, {:raw_ai_platform_audit_payload_forbidden, :prompt_body}} =
             prompt_attrs()
             |> Map.put(:prompt_body, "raw prompt")
             |> AIPlatformFact.prompt_resolved()
  end

  test "guard evaluated and violated facts carry bounded guard refs" do
    assert {:ok, evaluated} = AIPlatformFact.guard_evaluated(guard_evaluated_attrs())

    assert evaluated.fact_kind == :guard_evaluated
    assert evaluated.payload["payload_kind"] == "input_prompt"
    assert evaluated.payload["chain_ref"] == "guard-chain://a"
    assert evaluated.payload["redaction_posture"] == "block"

    assert {:ok, violated} = AIPlatformFact.guard_violated(guard_violated_attrs())

    assert violated.fact_kind == :guard_violated
    assert violated.payload["violation_id"] == "guard-violation://a"
    assert violated.payload["severity"] == "block"

    assert {:error, {:raw_ai_platform_audit_payload_forbidden, :guard_violation_body}} =
             guard_violated_attrs()
             |> Map.put(:guard_violation_body, "raw guard")
             |> AIPlatformFact.guard_violated()
  end

  test "eval replay and drift facts carry ref-only context" do
    assert {:ok, eval_fact} = AIPlatformFact.eval_run_recorded(eval_attrs())

    assert eval_fact.fact_kind == :eval_run_recorded
    assert eval_fact.payload["verdict"] == "regress"
    refute Map.has_key?(eval_fact.payload, "model_output")

    assert {:ok, replay_fact} = AIPlatformFact.replay_executed(replay_attrs())

    assert replay_fact.fact_kind == :replay_executed
    assert replay_fact.payload["cost_class"] == "replay"

    assert {:ok, drift_fact} = AIPlatformFact.drift_signal_recorded(drift_attrs())

    assert drift_fact.fact_kind == :drift_signal_recorded
    assert drift_fact.payload["signal_class"] == "guard_decision_drift"

    assert {:error, {:raw_ai_platform_audit_payload_forbidden, :model_output}} =
             eval_attrs()
             |> Map.put(:model_output, "raw")
             |> AIPlatformFact.eval_run_recorded()

    assert {:error, {:invalid_ai_platform_audit_field, :cost_class}} =
             replay_attrs()
             |> Map.put(:cost_class, :production)
             |> AIPlatformFact.replay_executed()
  end

  defp memory_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-memory-audit",
      trace_ref: "trace://a",
      scope_key: "memory-scope://a",
      operation: :write,
      redaction_policy_ref: "redaction-policy://memory",
      memory_id: "memory://a",
      evidence_hash: "sha256:memory"
    }
  end

  defp budget_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-budget-audit",
      trace_ref: "trace://a",
      locus: :preflight,
      decision_class: :deny_exhausted,
      requested_units: 10,
      granted_units: 0,
      residual_units: 0,
      policy_revision_ref: "policy-revision://budget"
    }
  end

  defp prompt_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-prompt-audit",
      trace_ref: "trace://prompt",
      prompt_id: "prompt://a",
      revision: 2,
      ab_key: "subject-1",
      decision_class: :resolved
    }
  end

  defp guard_evaluated_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-guard-audit",
      trace_ref: "trace://guard",
      payload_kind: :input_prompt,
      chain_ref: "guard-chain://a",
      decision_class: :block,
      redaction_posture: :block
    }
  end

  defp guard_violated_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-guard-violation",
      trace_ref: "trace://guard",
      violation_id: "guard-violation://a",
      detector_ref: "detector://pii-reference",
      severity: :block,
      violation_class: "pii",
      redaction_posture: :block
    }
  end

  defp eval_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-eval-audit",
      trace_ref: "trace://eval",
      eval_run_ref: "eval-run://a",
      suite_ref: "eval-suite://a",
      variant_ref: "eval-variant://a",
      verdict: :regress,
      release_manifest_ref: "release://phase-c"
    }
  end

  defp replay_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-replay-audit",
      trace_ref: "trace://replay",
      source_trace_ref: "trace://source",
      replay_trace_ref: "trace://replay/1",
      replay_bundle_ref: "replay-bundle://a",
      decision_class: :diverged,
      cost_class: :replay
    }
  end

  defp drift_attrs do
    %{
      tenant_ref: "tenant://a",
      authority_ref: "authority://a",
      installation_ref: "installation://a",
      idempotency_key: "idem-drift-audit",
      trace_ref: "trace://drift",
      drift_signal_ref: "drift-signal://a",
      signal_class: :guard_decision_drift,
      magnitude_class: "bounded_delta",
      window_ref: "drift-window://a"
    }
  end
end
