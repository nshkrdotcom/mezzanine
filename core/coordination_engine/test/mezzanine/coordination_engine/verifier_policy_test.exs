defmodule Mezzanine.CoordinationEngine.VerifierPolicyTest do
  use ExUnit.Case, async: true

  alias Mezzanine.CoordinationEngine.VerifierPolicy

  test "binds verifier and termination policy to model profile, operation policy, replay, schema, and GEPA target refs" do
    assert {:ok, policy} = VerifierPolicy.new(policy_attrs())
    assert policy.verifier_model_profile_ref == "model://verifier"
    assert policy.operation_policy_ref == "operation-policy://verify"
    assert policy.gepa_target_refs == ["gepa-target://verifier/prompt"]

    assert {:ok, decision} =
             VerifierPolicy.evaluate(policy, %{
               verifier_result_ref: "verifier-result://1",
               score_schema_ref: "score-schema://binary",
               score_band: :terminate,
               replay_ref: "replay://verifier/1",
               trace_ref: "trace://verifier/1"
             })

    assert decision.termination_decision == :terminate
    assert decision.replay_ref == "replay://verifier/1"
    assert decision.score_schema_ref == "score-schema://binary"
  end

  test "fails closed for missing model or operation policy refs, schema mismatch, and raw verifier output" do
    assert {:error, {:missing_required_ref, :verifier_model_profile_ref}} =
             policy_attrs()
             |> Map.delete(:verifier_model_profile_ref)
             |> VerifierPolicy.new()

    assert {:error, {:missing_required_ref, :operation_policy_ref}} =
             policy_attrs()
             |> Map.delete(:operation_policy_ref)
             |> VerifierPolicy.new()

    {:ok, policy} = VerifierPolicy.new(policy_attrs())

    assert {:error, {:score_schema_mismatch, "score-schema://other"}} =
             VerifierPolicy.evaluate(policy, %{
               verifier_result_ref: "verifier-result://1",
               score_schema_ref: "score-schema://other",
               score_band: :pass,
               replay_ref: "replay://verifier/1",
               trace_ref: "trace://verifier/1"
             })

    assert {:error, {:forbidden_raw_field, :raw_model_output}} =
             VerifierPolicy.evaluate(policy, %{
               verifier_result_ref: "verifier-result://1",
               score_schema_ref: "score-schema://binary",
               score_band: :pass,
               replay_ref: "replay://verifier/1",
               trace_ref: "trace://verifier/1",
               raw_model_output: "raw"
             })
  end

  defp policy_attrs do
    %{
      verifier_policy_ref: "verifier-policy://worker",
      verifier_prompt_ref: "prompt://verifier",
      verifier_model_profile_ref: "model://verifier",
      operation_policy_ref: "operation-policy://verify",
      score_schema_ref: "score-schema://binary",
      termination_threshold_ref: "threshold://terminate",
      retry_policy_ref: "retry://verifier",
      repair_policy_ref: "repair://verifier",
      escalation_policy_ref: "escalation://human",
      replay_ref: "replay://verifier",
      trace_ref: "trace://verifier",
      gepa_target_refs: ["gepa-target://verifier/prompt"]
    }
  end
end
