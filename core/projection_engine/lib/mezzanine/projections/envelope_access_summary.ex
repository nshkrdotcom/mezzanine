defmodule Mezzanine.Projections.EnvelopeAccessSummary do
  @moduledoc """
  Projection-safe readback summary for payload and result envelopes.

  Projections expose durable storage facts and only expose inline data when the
  admitting boundary marked the envelope as already redacted for projection
  readback.
  """

  alias Mezzanine.Substrate.{PayloadEnvelope, ResultEnvelope}

  @enforce_keys [
    :envelope_kind,
    :envelope_ref,
    :storage_mode,
    :schema_ref,
    :redaction_ref,
    :readback_mode
  ]
  defstruct @enforce_keys ++
              [
                :data,
                :content_ref,
                :content_hash,
                :byte_size,
                :store_ref,
                :stream_ref,
                retention_refs: [],
                metadata: %{}
              ]

  @type t :: %__MODULE__{}

  @safe_inline_markers [:inline_redacted, "inline_redacted"]

  @spec from_payload(PayloadEnvelope.t()) :: t()
  def from_payload(%PayloadEnvelope{} = envelope) do
    from_envelope(:payload, envelope.payload_ref, envelope)
  end

  @spec from_result(ResultEnvelope.t()) :: t()
  def from_result(%ResultEnvelope{} = envelope) do
    from_envelope(:result, envelope.result_ref, envelope)
  end

  defp from_envelope(envelope_kind, envelope_ref, envelope) do
    %__MODULE__{
      envelope_kind: envelope_kind,
      envelope_ref: envelope_ref,
      storage_mode: envelope.storage_mode,
      schema_ref: envelope.schema_ref,
      redaction_ref: envelope.redaction_ref,
      readback_mode: readback_mode(envelope),
      data: readback_data(envelope),
      content_ref: envelope.content_ref,
      content_hash: envelope.content_hash,
      byte_size: envelope.byte_size,
      store_ref: envelope.store_ref,
      stream_ref: envelope.stream_ref,
      retention_refs: envelope.retention_refs || [],
      metadata: projection_metadata(envelope.metadata || %{})
    }
  end

  defp readback_mode(%{storage_mode: :inline} = envelope) do
    if inline_redacted?(envelope), do: :inline_redacted, else: :ref_only
  end

  defp readback_mode(%{storage_mode: :content_addressed}), do: :content_store_ref
  defp readback_mode(%{storage_mode: :stream}), do: :stream_ref
  defp readback_mode(_envelope), do: :ref_only

  defp readback_data(%{storage_mode: :inline} = envelope) do
    if inline_redacted?(envelope), do: envelope.data, else: nil
  end

  defp readback_data(_envelope), do: nil

  defp inline_redacted?(envelope) do
    metadata_value(envelope.metadata || %{}, :projection_readback) in @safe_inline_markers
  end

  defp projection_metadata(metadata) do
    [:content_owner_ref, :read_scope_ref, :projection_readback]
    |> Enum.reduce(%{}, fn key, acc ->
      case metadata_value(metadata, key) do
        nil -> acc
        value -> Map.put(acc, key, value)
      end
    end)
  end

  defp metadata_value(%{} = metadata, key) do
    case Map.fetch(metadata, key) do
      {:ok, value} -> value
      :error -> Map.get(metadata, Atom.to_string(key))
    end
  end
end
