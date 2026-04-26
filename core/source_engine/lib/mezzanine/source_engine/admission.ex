defmodule Mezzanine.SourceEngine.Admission do
  @moduledoc """
  Pure source-event admission and dedupe helpers.
  """

  alias Mezzanine.SourceEngine.{SourceBinding, SourceEvent}

  @required_fields [
    :installation_id,
    :source_binding_id,
    :provider,
    :external_ref,
    :event_kind,
    :provider_revision,
    :payload_schema,
    :trace_id,
    :causation_id
  ]

  @dispatchable_lifecycle_states ["submitted", "retry_submission"]
  @terminal_lifecycle_states ["completed", "rejected", "expired"]

  @spec admit(map(), MapSet.t(String.t())) ::
          {:ok, SourceEvent.t(), MapSet.t(String.t())}
          | {:duplicate, SourceEvent.t(), MapSet.t(String.t())}
          | {:error, {:missing_required, atom()}}
  def admit(attrs, seen_keys) when is_map(attrs) and is_struct(seen_keys, MapSet) do
    with :ok <- validate_required(attrs) do
      event = build_event(attrs)

      if MapSet.member?(seen_keys, event.idempotency_key) do
        {:duplicate, %{event | status: :duplicate}, seen_keys}
      else
        {:ok, event, MapSet.put(seen_keys, event.idempotency_key)}
      end
    end
  end

  @spec classify_candidate(map(), SourceBinding.t() | map()) ::
          {:submitted | :candidate | :ignored, map()}
  def classify_candidate(payload, binding) when is_map(payload) and is_map(binding) do
    source_state = source_state(payload)
    mapping = state_mapping(binding)
    canonical_state = canonical_lifecycle_state(source_state, mapping)

    classify_routed(payload, mapping, source_state, canonical_state)
  end

  defp validate_required(attrs) do
    case Enum.find(@required_fields, &blank?(value(attrs, &1))) do
      nil -> :ok
      field -> {:error, {:missing_required, field}}
    end
  end

  defp decision(lifecycle_state, canonical_state, source_state, reason, blocker_refs) do
    %{
      lifecycle_state: lifecycle_state,
      canonical_state: canonical_state,
      source_state: source_state,
      reason: reason,
      blocker_refs: blocker_refs
    }
  end

  defp classify_routed(_payload, _mapping, source_state, _canonical_state)
       when source_state in [nil, ""] do
    {:ignored, decision("ignored", nil, source_state, :missing_source_state, [])}
  end

  defp classify_routed(payload, mapping, source_state, canonical_state) do
    if routed_to_worker?(payload) do
      classify_mapped(payload, mapping, source_state, canonical_state)
    else
      {:ignored, decision("ignored", canonical_state, source_state, :not_routed_to_worker, [])}
    end
  end

  defp classify_mapped(_payload, _mapping, source_state, nil) do
    {:ignored, decision("ignored", nil, source_state, :unmapped_source_state, [])}
  end

  defp classify_mapped(_payload, _mapping, source_state, canonical_state)
       when canonical_state in @terminal_lifecycle_states do
    {:ignored, decision("ignored", canonical_state, source_state, :terminal_source_state, [])}
  end

  defp classify_mapped(payload, mapping, source_state, canonical_state)
       when canonical_state in @dispatchable_lifecycle_states do
    dispatch_decision(payload, mapping, source_state, canonical_state)
  end

  defp classify_mapped(_payload, _mapping, source_state, canonical_state) do
    {:candidate, decision("candidate", canonical_state, source_state, :non_dispatch_state, [])}
  end

  defp dispatch_decision(payload, mapping, source_state, canonical_state) do
    case non_terminal_blockers(payload, terminal_source_states(mapping)) do
      [] ->
        {:submitted, decision("submitted", canonical_state, source_state, :dispatchable, [])}

      blocker_refs ->
        {:candidate,
         decision(
           "candidate",
           canonical_state,
           source_state,
           :blocked_by_non_terminal,
           blocker_refs
         )}
    end
  end

  defp source_state(payload) do
    value(payload, :state) ||
      value(payload, :state_name) ||
      value(payload, :source_state) |> nested_state_name()
  end

  defp nested_state_name(%{"name" => name}) when is_binary(name), do: name
  defp nested_state_name(%{name: name}) when is_binary(name), do: name
  defp nested_state_name(value) when is_binary(value), do: value
  defp nested_state_name(_value), do: nil

  defp state_mapping(%SourceBinding{state_mapping: mapping}), do: mapping || %{}
  defp state_mapping(binding), do: value(binding, :state_mapping) || %{}

  defp canonical_lifecycle_state(nil, _mapping), do: nil

  defp canonical_lifecycle_state(source_state, mapping) do
    mapping
    |> Enum.find(fn {_canonical_state, provider_states} ->
      provider_states
      |> List.wrap()
      |> Enum.any?(&same_state?(&1, source_state))
    end)
    |> case do
      {canonical_state, _provider_states} -> to_string(canonical_state)
      nil -> nil
    end
  end

  defp terminal_source_states(mapping) do
    mapping
    |> Enum.flat_map(fn {canonical_state, provider_states} ->
      if to_string(canonical_state) in @terminal_lifecycle_states do
        List.wrap(provider_states)
      else
        []
      end
    end)
  end

  defp routed_to_worker?(payload) do
    case value(payload, :assigned_to_worker) do
      false -> false
      _other -> true
    end
  end

  defp non_terminal_blockers(payload, terminal_states) do
    payload
    |> blocker_refs()
    |> Enum.reject(&terminal_blocker?(&1, terminal_states))
  end

  defp blocker_refs(payload) do
    payload
    |> value(:blocked_by)
    |> case do
      nil -> value(payload, :blockers)
      blockers -> blockers
    end
    |> List.wrap()
    |> Enum.filter(&is_map/1)
  end

  defp terminal_blocker?(blocker, terminal_states) do
    case source_state(blocker) do
      state when is_binary(state) -> Enum.any?(terminal_states, &same_state?(&1, state))
      _other -> false
    end
  end

  defp same_state?(left, right), do: normalize_state(left) == normalize_state(right)

  defp normalize_state(value) when is_binary(value),
    do: value |> String.trim() |> String.downcase()

  defp normalize_state(value), do: value |> to_string() |> normalize_state()

  defp build_event(attrs) do
    normalized_payload = value(attrs, :normalized_payload) || %{}
    idempotency_key = value(attrs, :idempotency_key) || idempotency_key(attrs)
    source_event_id = "src_" <> digest(idempotency_key, 24)

    %SourceEvent{
      source_event_id: source_event_id,
      installation_id: value(attrs, :installation_id),
      source_binding_id: value(attrs, :source_binding_id),
      provider: value(attrs, :provider),
      external_ref: value(attrs, :external_ref),
      event_kind: value(attrs, :event_kind),
      provider_revision: value(attrs, :provider_revision),
      payload_schema: value(attrs, :payload_schema),
      payload_hash: "sha256:" <> digest(normalized_payload),
      idempotency_key: idempotency_key,
      trace_id: value(attrs, :trace_id),
      causation_id: value(attrs, :causation_id),
      occurred_at: value(attrs, :occurred_at),
      normalized_payload: normalized_payload,
      provider_payload_ref: value(attrs, :provider_payload_ref)
    }
  end

  defp idempotency_key(attrs) do
    [
      value(attrs, :provider),
      value(attrs, :source_binding_id),
      value(attrs, :external_ref),
      value(attrs, :event_kind),
      value(attrs, :provider_revision)
    ]
    |> Enum.join("/")
  end

  defp value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))

  defp blank?(value), do: value in [nil, ""]

  defp digest(value, length \\ 64) do
    value
    |> canonical()
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, length)
  end

  defp canonical(map) when is_map(map) do
    map
    |> Enum.map(fn {key, value} -> {to_string(key), canonical(value)} end)
    |> Enum.sort_by(&elem(&1, 0))
  end

  defp canonical(list) when is_list(list), do: Enum.map(list, &canonical/1)
  defp canonical(value), do: value
end
