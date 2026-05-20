defmodule Mezzanine.Core.GovernedEffects.Projection do
  @moduledoc """
  Product-safe governed-effect readback.

  Projections intentionally expose lifecycle, authority, receipt, evidence, and
  trace facts only. Lower adapter internals and credential-bearing material are
  not copied into this surface.
  """

  alias Mezzanine.Core.GovernedEffects.Coordinator.Run
  alias Mezzanine.Core.GovernedEffects.EffectLog
  alias Mezzanine.Core.GovernedEffects.EffectLog.Entry
  alias Mezzanine.Core.GovernedEffects.EffectReceipt
  alias Mezzanine.Core.GovernedEffects.GovernedEffect

  @spec product_safe(Run.t()) :: map()
  def product_safe(%Run{} = run) do
    %{}
    |> put_effect(run.effect)
    |> put_authority(run.authority_packet, run.authority_metadata)
    |> put_receipt(run.receipt)
    |> put_log(run.log)
    |> drop_empty()
  end

  defp put_effect(projection, %GovernedEffect{} = effect) do
    projection
    |> Map.put("effect_ref", effect.effect_ref)
    |> Map.put("effect_type", effect.effect_type)
    |> Map.put("tenant_ref", effect.tenant_ref)
    |> Map.put("status", Atom.to_string(effect.status))
    |> Map.put("trace_ref", effect.trace_ref)
  end

  defp put_effect(projection, _effect), do: projection

  defp put_authority(projection, nil, _metadata), do: projection

  defp put_authority(projection, authority_packet, metadata) do
    projection
    |> Map.put("authority_ref", authority_packet.authority_ref)
    |> Map.put("authority_decision", Atom.to_string(authority_packet.decision))
    |> put_optional("authority_decision_hash", Map.get(metadata, "decision_hash"))
    |> put_optional("boundary_class", Map.get(metadata, "boundary_class"))
    |> put_optional("posture", Map.get(metadata, "posture"))
  end

  defp put_receipt(projection, nil), do: projection

  defp put_receipt(projection, %EffectReceipt{} = receipt) do
    projection
    |> Map.put("receipt_ref", receipt.receipt_ref)
    |> Map.put("receipt_status", Atom.to_string(receipt.status))
    |> Map.put("evidence_refs", receipt.evidence_refs || [])
  end

  defp put_log(projection, nil), do: projection

  defp put_log(projection, %EffectLog{} = log) do
    projection
    |> Map.put("timeline", Enum.map(EffectLog.entries(log), &entry_view/1))
    |> Map.put("trace_summary_hash", EffectLog.trace_summary_hash(log))
  end

  defp entry_view(%Entry{} = entry) do
    %{
      "sequence" => entry.sequence,
      "event_kind" => Atom.to_string(entry.event_kind),
      "status" => status_string(entry.status),
      "entry_hash" => entry.entry_hash,
      "parent_evidence_hash" => entry.parent_evidence_hash
    }
    |> drop_empty()
  end

  defp status_string(nil), do: nil
  defp status_string(status) when is_atom(status), do: Atom.to_string(status)
  defp status_string(status), do: status

  defp put_optional(projection, _key, nil), do: projection
  defp put_optional(projection, key, value), do: Map.put(projection, key, value)

  defp drop_empty(map) do
    Map.reject(map, fn {_key, value} ->
      is_nil(value) or value == %{} or value == []
    end)
  end
end
