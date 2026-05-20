defmodule Mezzanine.Core.GovernedEffects.TransitionGateAndEffectLogTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Core.GovernedEffects.EffectLog
  alias Mezzanine.Core.GovernedEffects.GovernedEffect
  alias Mezzanine.Core.GovernedEffects.TransitionGate

  test "TransitionGate accepts valid authorization and dispatch transitions" do
    proposed = governed_effect()

    assert {:ok, authorized} =
             TransitionGate.transition(proposed, :authorized,
               authority_ref: "authority://tenant-a/diagnostic/review",
               expected_version: 1
             )

    assert authorized.status == :authorized
    assert authorized.authority_ref == "authority://tenant-a/diagnostic/review"
    assert authorized.expected_version == 2

    assert {:ok, dispatched} =
             TransitionGate.transition(authorized, :dispatched, expected_version: 2)

    assert dispatched.status == :dispatched
    assert dispatched.expected_version == 3
  end

  test "TransitionGate rejects invalid transitions and missing tenant context" do
    proposed = governed_effect()

    assert {:error, {:invalid_transition, :proposed, :completed}} =
             TransitionGate.transition(proposed, :completed)

    assert {:error, {:missing_tenant_ref, "effect://tenant-a/diagnostic/001"}} =
             proposed
             |> Map.put(:tenant_ref, nil)
             |> TransitionGate.transition(:authorized,
               authority_ref: "authority://tenant-a/diagnostic/review"
             )
  end

  test "TransitionGate rejects missing authority, version conflicts, and unregistered types" do
    proposed = governed_effect()

    assert {:error, {:missing_authority_ref, :authorized}} =
             TransitionGate.transition(%{proposed | authority_ref: nil}, :authorized)

    assert {:error, {:version_conflict, %{expected: 7, actual: 1}}} =
             TransitionGate.transition(proposed, :authorized,
               authority_ref: "authority://tenant-a/diagnostic/review",
               expected_version: 7
             )

    assert {:error, {:unregistered_effect_type, "unknown.effect"}} =
             proposed
             |> Map.put(:effect_type, "unknown.effect")
             |> TransitionGate.transition(:authorized,
               authority_ref: "authority://tenant-a/diagnostic/review",
               expected_version: 1
             )
  end

  test "EffectLog appends ordered hash-chained entries and rebuilds from entries" do
    assert {:ok, log} = EffectLog.new(trace_ref: "trace-tenant-a-diagnostic-001")

    assert {:ok, log, first} = EffectLog.append(log, log_event(:proposed))
    assert {:ok, log, second} = EffectLog.append(log, log_event(:authorized))

    assert first.sequence == 1
    assert first.parent_evidence_hash == nil
    assert second.sequence == 2
    assert second.parent_evidence_hash == first.entry_hash
    assert EffectLog.entries(log) == [first, second]
    assert :ok = EffectLog.verify(log)
    assert {:ok, rebuilt} = log |> EffectLog.entries() |> EffectLog.rebuild()
    assert EffectLog.entries(rebuilt) == [first, second]
  end

  test "EffectLog is append-only and rejects missing sequence entries" do
    {:ok, log} = EffectLog.new(trace_ref: "trace-tenant-a-diagnostic-001")
    {:ok, log, _first} = EffectLog.append(log, log_event(:proposed))
    {:ok, _log, second} = EffectLog.append(log, log_event(:authorized))

    refute function_exported?(EffectLog, :update, 3)
    refute function_exported?(EffectLog, :delete, 2)

    assert {:error, {:non_contiguous_sequence, %{expected: 1, actual: 2}}} =
             EffectLog.rebuild([second])
  end

  test "EffectLog detects tampering and computes trace summary hashes" do
    {:ok, log} = EffectLog.new(trace_ref: "trace-tenant-a-diagnostic-001")
    {:ok, log, first} = EffectLog.append(log, log_event(:proposed))
    {:ok, log, second} = EffectLog.append(log, log_event(:authorized))

    tampered = %{second | payload: %{"status" => "tampered"}}

    assert {:error, {:entry_hash_mismatch, %{sequence: 2}}} =
             EffectLog.verify([first, tampered])

    assert "sha256:" <> _hash = EffectLog.trace_summary_hash(log)
    assert EffectLog.trace_summary_hash([second, first]) == EffectLog.trace_summary_hash(log)
  end

  test "EffectLog quarantine appends a quarantine entry without mutating the target" do
    {:ok, log} = EffectLog.new(trace_ref: "trace-tenant-a-diagnostic-001")
    {:ok, log, first} = EffectLog.append(log, log_event(:proposed))

    assert {:ok, log, quarantine} = EffectLog.quarantine(log, 1, "unexpected payload")

    assert quarantine.sequence == 2
    assert quarantine.event_kind == :quarantine
    assert quarantine.parent_evidence_hash == first.entry_hash

    assert quarantine.payload == %{
             "reason" => "unexpected payload",
             "target_entry_hash" => first.entry_hash,
             "target_sequence" => 1
           }

    assert EffectLog.entries(log) |> hd() == first
    assert :ok = EffectLog.verify(log)
  end

  defp governed_effect(attrs \\ %{}) do
    %{
      effect_ref: "effect://tenant-a/diagnostic/001",
      effect_type: "diagnostic",
      command_ref: "command://tenant-a/diagnostic/001",
      tenant_ref: "tenant-a",
      actor_ref: "actor://user/operator-a",
      status: :proposed,
      expected_version: 1,
      trace_ref: "trace-tenant-a-diagnostic-001"
    }
    |> Map.merge(attrs)
    |> GovernedEffect.new!()
  end

  defp log_event(status) do
    %{
      effect_ref: "effect://tenant-a/diagnostic/001",
      tenant_ref: "tenant-a",
      trace_ref: "trace-tenant-a-diagnostic-001",
      event_kind: "effect_transition",
      status: status,
      payload: %{"status" => Atom.to_string(status)}
    }
  end
end
