defmodule Mezzanine.Audit.AIPlatformFact do
  @moduledoc """
  Redaction-safe AI Platform audit fact builders.

  These builders produce bounded audit fact attributes for callers that own the
  transaction boundary. They never accept raw memory bodies, prompt bodies,
  provider payloads, or secret-like material.
  """

  @memory_operations [:write, :read, :evict]
  @budget_loci [:preflight, :append, :stream, :runtime_admission, :reconciliation]
  @budget_decisions [
    :allow,
    :allow_warn_soft,
    :allow_with_override,
    :allow_with_redaction,
    :deny_hard_exhausted,
    :deny_oversize,
    :deny_exhausted,
    :deny_policy,
    :deny_revoked
  ]
  @prompt_decisions [
    :resolved,
    :resolved_with_redaction,
    :denied_revoked,
    :denied_revision_missing,
    :denied_ab_assignment_invalid,
    :denied_policy
  ]
  @guard_payload_kinds [
    :input_prompt,
    :tool_input,
    :tool_output,
    :provider_response,
    :memory_candidate
  ]
  @guard_decisions [
    :allow,
    :allow_with_redaction,
    :block,
    :escalate,
    :deny_policy,
    :deny_detector_unavailable
  ]
  @guard_postures [:pass, :partial, :excerpt_only, :no_export, :block]
  @guard_severities [:info, :warn, :block, :escalate]
  @eval_verdicts [:pass, :regress, :improve, :inconclusive]
  @replay_decisions [:clean, :diverged, :denied, :inconclusive]
  @drift_signal_classes [
    :prompt_drift,
    :tool_call_drift,
    :guard_decision_drift,
    :memory_access_drift,
    :cost_attribution_drift,
    :latency_drift
  ]
  @cost_classes [:production, :replay, :eval, :simulation, :infrastructure]
  @amount_classes [
    :production_native,
    :redacted_below_floor,
    :redacted_above_ceiling,
    :bounded_excerpt
  ]
  @raw_payload_keys [
    :body,
    :raw_body,
    :memory_body,
    :raw_memory_body,
    :content,
    :raw_content,
    :prompt_body,
    :guard_payload,
    :guard_violation_body,
    :guard_violation_payload,
    :provider_payload,
    :eval_payload,
    :model_output,
    :replay_divergence_excerpt,
    :raw_guard,
    :secret,
    :token,
    :cost_amount,
    :raw_amount,
    "body",
    "raw_body",
    "memory_body",
    "raw_memory_body",
    "content",
    "raw_content",
    "prompt_body",
    "guard_payload",
    "guard_violation_body",
    "guard_violation_payload",
    "provider_payload",
    "eval_payload",
    "model_output",
    "replay_divergence_excerpt",
    "raw_guard",
    "secret",
    "token",
    "cost_amount",
    "raw_amount"
  ]

  @memory_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :scope_key,
    :operation,
    :redaction_policy_ref,
    :memory_id,
    :evidence_hash
  ]

  @budget_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :locus,
    :decision_class,
    :requested_units,
    :granted_units,
    :residual_units,
    :policy_revision_ref
  ]

  @prompt_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :prompt_id,
    :revision,
    :ab_key,
    :decision_class
  ]

  @guard_evaluated_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :payload_kind,
    :chain_ref,
    :decision_class,
    :redaction_posture
  ]

  @guard_violated_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :violation_id,
    :detector_ref,
    :severity,
    :violation_class,
    :redaction_posture
  ]

  @eval_run_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :eval_run_ref,
    :suite_ref,
    :variant_ref,
    :verdict,
    :release_manifest_ref
  ]

  @replay_executed_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :source_trace_ref,
    :replay_trace_ref,
    :replay_bundle_ref,
    :decision_class,
    :cost_class
  ]

  @drift_signal_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :drift_signal_ref,
    :signal_class,
    :magnitude_class,
    :window_ref
  ]

  @cost_recorded_required [
    :tenant_ref,
    :authority_ref,
    :installation_ref,
    :idempotency_key,
    :trace_ref,
    :run_ref,
    :capability_id,
    :cost_class,
    :amount_class,
    :token_meter_ref,
    :release_manifest_ref
  ]

  @spec memory_access_recorded(map()) :: {:ok, map()} | {:error, term()}
  def memory_access_recorded(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @memory_required),
         {:ok, operation} <- member(attrs, :operation, @memory_operations) do
      {:ok,
       %{
         installation_id: fetch!(attrs, :installation_ref),
         trace_id: fetch!(attrs, :trace_ref),
         fact_kind: :memory_access_recorded,
         actor_ref: %{
           "tenant_ref" => fetch!(attrs, :tenant_ref),
           "authority_ref" => fetch!(attrs, :authority_ref)
         },
         idempotency_key: fetch!(attrs, :idempotency_key),
         payload: %{
           "tenant_ref" => fetch!(attrs, :tenant_ref),
           "authority_ref" => fetch!(attrs, :authority_ref),
           "installation_ref" => fetch!(attrs, :installation_ref),
           "scope_key" => fetch!(attrs, :scope_key),
           "operation" => Atom.to_string(operation),
           "redaction_policy_ref" => fetch!(attrs, :redaction_policy_ref),
           "memory_id" => fetch!(attrs, :memory_id),
           "evidence_hash" => fetch!(attrs, :evidence_hash)
         }
       }}
    end
  end

  @spec budget_enforced(map()) :: {:ok, map()} | {:error, term()}
  def budget_enforced(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @budget_required),
         {:ok, locus} <- member(attrs, :locus, @budget_loci),
         {:ok, decision_class} <- member(attrs, :decision_class, @budget_decisions),
         :ok <- non_negative_integer(attrs, :requested_units),
         :ok <- non_negative_integer(attrs, :granted_units),
         :ok <- non_negative_integer(attrs, :residual_units) do
      {:ok,
       %{
         installation_id: fetch!(attrs, :installation_ref),
         trace_id: fetch!(attrs, :trace_ref),
         fact_kind: :budget_enforced,
         actor_ref: %{
           "tenant_ref" => fetch!(attrs, :tenant_ref),
           "authority_ref" => fetch!(attrs, :authority_ref)
         },
         idempotency_key: fetch!(attrs, :idempotency_key),
         payload: %{
           "tenant_ref" => fetch!(attrs, :tenant_ref),
           "authority_ref" => fetch!(attrs, :authority_ref),
           "installation_ref" => fetch!(attrs, :installation_ref),
           "locus" => Atom.to_string(locus),
           "decision_class" => Atom.to_string(decision_class),
           "requested_units" => fetch!(attrs, :requested_units),
           "granted_units" => fetch!(attrs, :granted_units),
           "residual_units" => fetch!(attrs, :residual_units),
           "policy_revision_ref" => fetch!(attrs, :policy_revision_ref)
         }
       }}
    end
  end

  @spec cost_recorded(map()) :: {:ok, map()} | {:error, term()}
  def cost_recorded(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @cost_recorded_required),
         {:ok, cost_class} <- member(attrs, :cost_class, @cost_classes),
         {:ok, amount_class} <- member(attrs, :amount_class, @amount_classes) do
      {:ok,
       base_fact(attrs, :cost_recorded, %{
         "run_ref" => fetch!(attrs, :run_ref),
         "capability_id" => fetch!(attrs, :capability_id),
         "cost_class" => Atom.to_string(cost_class),
         "amount_class" => Atom.to_string(amount_class),
         "token_meter_ref" => fetch!(attrs, :token_meter_ref),
         "release_manifest_ref" => fetch!(attrs, :release_manifest_ref)
       })}
    end
  end

  @spec prompt_resolved(map()) :: {:ok, map()} | {:error, term()}
  def prompt_resolved(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @prompt_required),
         {:ok, decision_class} <- member(attrs, :decision_class, @prompt_decisions),
         :ok <- positive_integer(attrs, :revision) do
      {:ok,
       base_fact(attrs, :prompt_resolved, %{
         "prompt_id" => fetch!(attrs, :prompt_id),
         "revision" => fetch!(attrs, :revision),
         "ab_key" => fetch!(attrs, :ab_key),
         "decision_class" => Atom.to_string(decision_class)
       })}
    end
  end

  @spec guard_evaluated(map()) :: {:ok, map()} | {:error, term()}
  def guard_evaluated(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @guard_evaluated_required),
         {:ok, payload_kind} <- member(attrs, :payload_kind, @guard_payload_kinds),
         {:ok, decision_class} <- member(attrs, :decision_class, @guard_decisions),
         {:ok, redaction_posture} <- member(attrs, :redaction_posture, @guard_postures) do
      {:ok,
       base_fact(attrs, :guard_evaluated, %{
         "payload_kind" => Atom.to_string(payload_kind),
         "chain_ref" => fetch!(attrs, :chain_ref),
         "decision_class" => Atom.to_string(decision_class),
         "redaction_posture" => Atom.to_string(redaction_posture)
       })}
    end
  end

  @spec guard_violated(map()) :: {:ok, map()} | {:error, term()}
  def guard_violated(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @guard_violated_required),
         {:ok, severity} <- member(attrs, :severity, @guard_severities),
         {:ok, redaction_posture} <- member(attrs, :redaction_posture, @guard_postures) do
      {:ok,
       base_fact(attrs, :guard_violated, %{
         "violation_id" => fetch!(attrs, :violation_id),
         "detector_ref" => fetch!(attrs, :detector_ref),
         "severity" => Atom.to_string(severity),
         "violation_class" => fetch!(attrs, :violation_class),
         "redaction_posture" => Atom.to_string(redaction_posture)
       })}
    end
  end

  @spec eval_run_recorded(map()) :: {:ok, map()} | {:error, term()}
  def eval_run_recorded(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @eval_run_required),
         {:ok, verdict} <- member(attrs, :verdict, @eval_verdicts) do
      {:ok,
       base_fact(attrs, :eval_run_recorded, %{
         "eval_run_ref" => fetch!(attrs, :eval_run_ref),
         "suite_ref" => fetch!(attrs, :suite_ref),
         "variant_ref" => fetch!(attrs, :variant_ref),
         "verdict" => Atom.to_string(verdict),
         "release_manifest_ref" => fetch!(attrs, :release_manifest_ref)
       })}
    end
  end

  @spec replay_executed(map()) :: {:ok, map()} | {:error, term()}
  def replay_executed(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @replay_executed_required),
         {:ok, decision_class} <- member(attrs, :decision_class, @replay_decisions),
         :ok <- replay_cost_class(attrs) do
      {:ok,
       base_fact(attrs, :replay_executed, %{
         "source_trace_ref" => fetch!(attrs, :source_trace_ref),
         "replay_trace_ref" => fetch!(attrs, :replay_trace_ref),
         "replay_bundle_ref" => fetch!(attrs, :replay_bundle_ref),
         "decision_class" => Atom.to_string(decision_class),
         "cost_class" => "replay"
       })}
    end
  end

  @spec drift_signal_recorded(map()) :: {:ok, map()} | {:error, term()}
  def drift_signal_recorded(attrs) when is_map(attrs) do
    with :ok <- reject_raw_payload(attrs),
         :ok <- required_fields(attrs, @drift_signal_required),
         {:ok, signal_class} <- member(attrs, :signal_class, @drift_signal_classes) do
      {:ok,
       base_fact(attrs, :drift_signal_recorded, %{
         "drift_signal_ref" => fetch!(attrs, :drift_signal_ref),
         "signal_class" => Atom.to_string(signal_class),
         "magnitude_class" => fetch!(attrs, :magnitude_class),
         "window_ref" => fetch!(attrs, :window_ref)
       })}
    end
  end

  defp base_fact(attrs, fact_kind, payload) do
    %{
      installation_id: fetch!(attrs, :installation_ref),
      trace_id: fetch!(attrs, :trace_ref),
      fact_kind: fact_kind,
      actor_ref: %{
        "tenant_ref" => fetch!(attrs, :tenant_ref),
        "authority_ref" => fetch!(attrs, :authority_ref)
      },
      idempotency_key: fetch!(attrs, :idempotency_key),
      payload:
        Map.merge(
          %{
            "tenant_ref" => fetch!(attrs, :tenant_ref),
            "authority_ref" => fetch!(attrs, :authority_ref),
            "installation_ref" => fetch!(attrs, :installation_ref)
          },
          payload
        )
    }
  end

  defp reject_raw_payload(attrs) do
    case Enum.find(@raw_payload_keys, &Map.has_key?(attrs, &1)) do
      nil -> :ok
      key -> {:error, {:raw_ai_platform_audit_payload_forbidden, key}}
    end
  end

  defp required_fields(attrs, fields) do
    case Enum.find(fields, &(not present?(fetch(attrs, &1)))) do
      nil -> :ok
      field -> {:error, {:missing_ai_platform_audit_ref, field}}
    end
  end

  defp member(attrs, field, allowed) do
    value = fetch(attrs, field)

    if value in allowed,
      do: {:ok, value},
      else: {:error, {:invalid_ai_platform_audit_field, field}}
  end

  defp replay_cost_class(attrs) do
    case fetch(attrs, :cost_class) do
      :replay -> :ok
      "replay" -> :ok
      _other -> {:error, {:invalid_ai_platform_audit_field, :cost_class}}
    end
  end

  defp non_negative_integer(attrs, field) do
    case fetch(attrs, field) do
      value when is_integer(value) and value >= 0 -> :ok
      _value -> {:error, {:invalid_ai_platform_audit_field, field}}
    end
  end

  defp positive_integer(attrs, field) do
    case fetch(attrs, field) do
      value when is_integer(value) and value > 0 -> :ok
      _value -> {:error, {:invalid_ai_platform_audit_field, field}}
    end
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)

  defp fetch!(attrs, field), do: fetch(attrs, field)
  defp fetch(attrs, field), do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))
end
