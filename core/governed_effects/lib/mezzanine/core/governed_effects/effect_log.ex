defmodule Mezzanine.Core.GovernedEffects.EffectLog do
  @moduledoc """
  Append-only in-memory governed-effect log with hash-chain verification.
  """

  alias Mezzanine.Core.GovernedEffects.EffectLog.Entry
  alias Mezzanine.Core.GovernedEffects.EffectReceipt
  alias Mezzanine.Core.GovernedEffects.GovernedEffect
  alias Mezzanine.Core.GovernedEffects.Support

  @event_kinds [
    :effect_transition,
    :receipt_reduced,
    :projection_updated,
    :quarantine,
    :diagnostic
  ]
  @required_event_fields [:effect_ref, :tenant_ref, :trace_ref, :event_kind, :payload]
  @statuses GovernedEffect.statuses() ++ EffectReceipt.statuses()

  defstruct trace_ref: nil, entries: []

  @type t :: %__MODULE__{trace_ref: String.t() | nil, entries: [Entry.t()]}

  @spec new(keyword() | map()) :: {:ok, t()} | {:error, term()}
  def new(attrs \\ []) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs) do
      {:ok, %__MODULE__{trace_ref: Support.optional(attrs, :trace_ref), entries: []}}
    end
  end

  @spec append(t(), keyword() | map()) :: {:ok, t(), Entry.t()} | {:error, term()}
  def append(%__MODULE__{} = log, attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- Support.require_fields(attrs, @required_event_fields),
         {:ok, event_kind} <-
           Support.bounded_atom(
             Support.required(attrs, :event_kind),
             @event_kinds,
             :invalid_event_kind
           ),
         {:ok, status} <- optional_status(Support.optional(attrs, :status)),
         {:ok, payload} <- normalized_payload(Support.required(attrs, :payload)),
         :ok <- validate_trace(log, Support.required(attrs, :trace_ref)) do
      entry =
        %Entry{
          sequence: length(log.entries) + 1,
          effect_ref: Support.required(attrs, :effect_ref),
          tenant_ref: Support.required(attrs, :tenant_ref),
          trace_ref: Support.required(attrs, :trace_ref),
          event_kind: event_kind,
          status: status,
          payload: payload,
          parent_evidence_hash: parent_hash(log),
          entry_hash: "",
          occurred_at: Support.optional(attrs, :occurred_at)
        }
        |> put_entry_hash()

      next_log = %{
        log
        | trace_ref: log.trace_ref || entry.trace_ref,
          entries: log.entries ++ [entry]
      }

      with :ok <- verify(next_log) do
        {:ok, next_log, entry}
      end
    end
  end

  @spec entries(t()) :: [Entry.t()]
  def entries(%__MODULE__{} = log), do: log.entries

  @spec rebuild([Entry.t()]) :: {:ok, t()} | {:error, term()}
  def rebuild(entries) when is_list(entries) do
    with :ok <- verify(entries) do
      {:ok, %__MODULE__{trace_ref: trace_ref(entries), entries: entries}}
    end
  end

  @spec verify(t() | [Entry.t()]) :: :ok | {:error, term()}
  def verify(%__MODULE__{} = log), do: verify(log.entries)

  def verify(entries) when is_list(entries) do
    entries
    |> Enum.reduce_while({:ok, nil, 1}, fn
      %Entry{} = entry, {:ok, prior_hash, expected_sequence} ->
        with :ok <- validate_sequence(entry, expected_sequence),
             :ok <- validate_parent(entry, prior_hash),
             :ok <- validate_entry_hash(entry) do
          {:cont, {:ok, entry.entry_hash, expected_sequence + 1}}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end

      _entry, _acc ->
        {:halt, {:error, :invalid_effect_log_entry}}
    end)
    |> case do
      {:ok, _prior_hash, _next_sequence} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @spec quarantine(t(), pos_integer(), String.t()) :: {:ok, t(), Entry.t()} | {:error, term()}
  def quarantine(%__MODULE__{} = log, sequence, reason)
      when is_integer(sequence) and sequence > 0 do
    case Enum.find(log.entries, &(&1.sequence == sequence)) do
      %Entry{} = entry ->
        append(log, %{
          effect_ref: entry.effect_ref,
          tenant_ref: entry.tenant_ref,
          trace_ref: entry.trace_ref,
          event_kind: :quarantine,
          payload: %{
            "reason" => reason,
            "target_entry_hash" => entry.entry_hash,
            "target_sequence" => entry.sequence
          }
        })

      nil ->
        {:error, {:entry_not_found, sequence}}
    end
  end

  def quarantine(_log, sequence, _reason), do: {:error, {:invalid_sequence, sequence}}

  @spec trace_summary_hash(t() | [Entry.t()]) :: String.t()
  def trace_summary_hash(%__MODULE__{} = log), do: trace_summary_hash(log.entries)

  def trace_summary_hash(entries) when is_list(entries) do
    entries
    |> Enum.map(& &1.entry_hash)
    |> Enum.sort()
    |> Support.digest()
  end

  defp optional_status(nil), do: {:ok, nil}
  defp optional_status(status), do: Support.bounded_atom(status, @statuses, :invalid_status)

  defp normalized_payload(payload) do
    payload = Support.dump_value(payload)

    with :ok <- Support.ensure_serializable(payload) do
      {:ok, payload}
    end
  end

  defp validate_trace(%__MODULE__{trace_ref: nil}, trace_ref)
       when is_binary(trace_ref) and trace_ref != "",
       do: :ok

  defp validate_trace(%__MODULE__{trace_ref: trace_ref}, trace_ref)
       when is_binary(trace_ref) and trace_ref != "",
       do: :ok

  defp validate_trace(%__MODULE__{trace_ref: expected}, actual),
    do: {:error, {:trace_ref_mismatch, %{expected: expected, actual: actual}}}

  defp parent_hash(%__MODULE__{entries: []}), do: nil

  defp parent_hash(%__MODULE__{entries: entries}),
    do: entries |> List.last() |> Map.fetch!(:entry_hash)

  defp put_entry_hash(%Entry{} = entry),
    do: %{entry | entry_hash: Support.digest(Entry.hash_material(entry))}

  defp validate_sequence(%Entry{sequence: expected_sequence}, expected_sequence), do: :ok

  defp validate_sequence(%Entry{sequence: actual}, expected),
    do: {:error, {:non_contiguous_sequence, %{expected: expected, actual: actual}}}

  defp validate_parent(%Entry{parent_evidence_hash: expected}, expected), do: :ok

  defp validate_parent(%Entry{} = entry, expected) do
    {:error,
     {:parent_evidence_hash_mismatch,
      %{sequence: entry.sequence, expected: expected, actual: entry.parent_evidence_hash}}}
  end

  defp validate_entry_hash(%Entry{} = entry) do
    expected_hash =
      entry |> Map.put(:entry_hash, "") |> put_entry_hash() |> Map.fetch!(:entry_hash)

    if entry.entry_hash == expected_hash do
      :ok
    else
      {:error, {:entry_hash_mismatch, %{sequence: entry.sequence}}}
    end
  end

  defp trace_ref([]), do: nil
  defp trace_ref([%Entry{trace_ref: trace_ref} | _entries]), do: trace_ref
end
