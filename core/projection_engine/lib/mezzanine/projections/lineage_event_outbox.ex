defmodule Mezzanine.Projections.LineageEventOutbox do
  @moduledoc """
  Builds and persists ref-only core lineage events for projection replay.

  The event shape is intentionally aligned with AITrace replay contracts without
  making projection reduction depend on AITrace packages.
  """

  alias Mezzanine.Projections.Store
  alias Mezzanine.Projections.SubjectRuntimeProjection
  alias Mezzanine.Substrate.{OperationReceipt, PayloadEnvelope, ResultEnvelope}

  @trace_level :core_lineage

  @type lineage_event :: map()

  @spec events_for_projection(SubjectRuntimeProjection.t(), [OperationReceipt.t()]) ::
          [lineage_event()]
  def events_for_projection(projection, receipts),
    do: events_for_projection(projection, receipts, [])

  @spec events_for_projection(SubjectRuntimeProjection.t(), [OperationReceipt.t()], keyword()) ::
          [lineage_event()]
  def events_for_projection(%SubjectRuntimeProjection{} = projection, receipts, opts)
      when is_list(receipts) and is_list(opts) do
    case Keyword.get(opts, :lineage_event_contract, :projection_reducer) do
      :full_execution -> full_execution_events(projection, receipts, opts)
      _contract -> projection_reducer_events(projection, receipts)
    end
  end

  defp projection_reducer_events(projection, receipts) do
    command = command_recorded_event(projection, receipts)

    operation_events =
      receipts
      |> Enum.sort_by(& &1.receipt_ref)
      |> Enum.with_index(1)
      |> Enum.flat_map(&operation_lineage_events(projection, &1, command.event_ref))

    projection_event =
      projection_updated_event(
        projection,
        receipts,
        Enum.map(operation_events, & &1),
        10_000 + length(operation_events)
      )

    [command | operation_events] ++ [projection_event]
  end

  defp full_execution_events(projection, receipts, opts) do
    command = command_recorded_event(projection, receipts)
    workflow = workflow_started_event(projection, receipts, command.event_ref)

    operation_events =
      receipts
      |> Enum.sort_by(& &1.receipt_ref)
      |> Enum.with_index(1)
      |> Enum.flat_map(&operation_execution_lineage_events(projection, &1, workflow.event_ref))

    review =
      review_event(projection, receipts, operation_terminal_event_refs(operation_events), opts)

    projection_event =
      execution_projection_updated_event(
        projection,
        receipts,
        operation_terminal_event_refs(operation_events) ++ [review.event_ref]
      )

    replay = replay_exported_event(projection, receipts, projection_event.event_ref)

    [command, workflow] ++ operation_events ++ [review, projection_event, replay]
  end

  @spec persist(SubjectRuntimeProjection.t(), [lineage_event()], keyword()) ::
          {:ok, map()} | {:error, term()}
  def persist(%SubjectRuntimeProjection{} = projection, events, opts \\ [])
      when is_list(events) and is_list(opts) do
    record_attrs = %{
      id: projection.projection_ref,
      projection_ref: projection.projection_ref,
      operation_context_ref: projection.operation_context_ref,
      subject_ref: projection.subject_ref,
      trace_ref: trace_ref(projection),
      events: []
    }

    with {:ok, record} <- Store.put_record(record_attrs, opts),
         {:ok, appended} <- append_events(record.id, events, opts) do
      {:ok,
       %{
         outbox_ref: "lineage-event-outbox://#{record.id}",
         record_id: record.id,
         event_count: length(appended),
         event_refs: Enum.map(appended, & &1.event_ref)
       }}
    end
  end

  defp append_events(record_id, events, opts) do
    Enum.reduce_while(events, {:ok, []}, fn event, {:ok, acc} ->
      case Store.append_event(record_id, event, opts) do
        {:ok, appended} -> {:cont, {:ok, [appended | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, appended} -> {:ok, Enum.reverse(appended)}
      error -> error
    end
  end

  defp command_recorded_event(projection, receipts) do
    causal_order = 1

    event(
      projection,
      :command_recorded,
      causal_order,
      root_event?: true,
      occurred_at: first_started_at(receipts) - 1,
      metadata_refs: %{
        operation_context_ref: projection.operation_context_ref,
        subject_ref: projection.subject_ref
      }
    )
  end

  defp operation_lineage_events(
         projection,
         {%OperationReceipt{} = receipt, index},
         command_event_ref
       ) do
    operation_requested =
      event(
        projection,
        :operation_requested,
        index * 10,
        predecessor_event_refs: [command_event_ref],
        operation_receipt: receipt,
        metadata_refs: operation_metadata_refs(receipt)
      )

    effect_receipted =
      event(
        projection,
        :effect_receipted,
        index * 10 + 1,
        predecessor_event_refs: [operation_requested.event_ref],
        operation_receipt: receipt,
        occurred_at: completed_at(receipt),
        metadata_refs: operation_metadata_refs(receipt)
      )

    receipt_reduced =
      event(
        projection,
        :receipt_reduced,
        index * 10 + 2,
        predecessor_event_refs: [effect_receipted.event_ref],
        operation_receipt: receipt,
        occurred_at: completed_at(receipt) + 1,
        metadata_refs:
          operation_metadata_refs(receipt)
          |> Map.put(:lineage_event_refs, receipt.lineage_event_refs)
      )

    [operation_requested, effect_receipted, receipt_reduced]
  end

  defp operation_execution_lineage_events(
         projection,
         {%OperationReceipt{} = receipt, index},
         workflow_event_ref
       ) do
    base_order = index * 100

    operation_requested =
      event(
        projection,
        :operation_requested,
        base_order,
        predecessor_event_refs: [workflow_event_ref],
        operation_receipt: receipt,
        metadata_refs: operation_metadata_refs(receipt)
      )

    manifest =
      event(
        projection,
        :jido_manifest_resolved,
        base_order + 1,
        predecessor_event_refs: [operation_requested.event_ref],
        operation_receipt: receipt,
        metadata_refs: operation_metadata_refs(receipt)
      )

    credential =
      event(
        projection,
        :credential_lease_materialized,
        base_order + 2,
        predecessor_event_refs: [manifest.event_ref],
        operation_receipt: receipt,
        metadata_refs: operation_metadata_refs(receipt)
      )

    effect_requested =
      event(
        projection,
        :effect_requested,
        base_order + 3,
        predecessor_event_refs: [credential.event_ref],
        operation_receipt: receipt,
        metadata_refs: operation_metadata_refs(receipt)
      )

    effect_receipted =
      event(
        projection,
        :effect_receipted,
        base_order + 4,
        predecessor_event_refs: [effect_requested.event_ref],
        operation_receipt: receipt,
        occurred_at: completed_at(receipt),
        metadata_refs: operation_metadata_refs(receipt)
      )

    receipt_reduced =
      event(
        projection,
        :receipt_reduced,
        base_order + 5,
        predecessor_event_refs: [effect_receipted.event_ref],
        operation_receipt: receipt,
        occurred_at: completed_at(receipt) + 1,
        metadata_refs:
          operation_metadata_refs(receipt)
          |> Map.put(:lineage_event_refs, receipt.lineage_event_refs)
      )

    events = [
      operation_requested,
      manifest,
      credential,
      effect_requested,
      effect_receipted,
      receipt_reduced
    ]

    if evidence_operation?(receipt) do
      events ++
        [evidence_attached_event(projection, receipt, receipt_reduced.event_ref, base_order + 6)]
    else
      events
    end
  end

  defp workflow_started_event(projection, receipts, command_event_ref) do
    event(
      projection,
      :workflow_started,
      2,
      predecessor_event_refs: [command_event_ref],
      occurred_at: first_started_at(receipts),
      metadata_refs: %{
        operation_context_ref: projection.operation_context_ref,
        subject_ref: projection.subject_ref,
        workflow_ref: "workflow://#{projection.operation_context_ref}"
      }
    )
  end

  defp evidence_attached_event(projection, receipt, receipt_reduced_event_ref, causal_order) do
    event(
      projection,
      :evidence_attached,
      causal_order,
      predecessor_event_refs: [receipt_reduced_event_ref],
      operation_receipt: receipt,
      occurred_at: completed_at(receipt) + 2,
      metadata_refs: evidence_metadata_refs(receipt)
    )
  end

  defp review_event(projection, receipts, predecessor_event_refs, opts) do
    event(
      projection,
      review_event_kind(Keyword.get(opts, :review_state, :skipped)),
      9_000,
      predecessor_event_refs: predecessor_event_refs,
      occurred_at: last_completed_at(receipts) + 3,
      metadata_refs: %{
        operation_context_ref: projection.operation_context_ref,
        subject_ref: projection.subject_ref,
        review_ref: "review://#{projection.operation_context_ref}"
      }
    )
  end

  defp execution_projection_updated_event(projection, receipts, predecessor_event_refs) do
    event(
      projection,
      :projection_updated,
      10_000,
      predecessor_event_refs: predecessor_event_refs,
      projection_visible?: true,
      projection_key: projection.projection_ref,
      occurred_at: last_completed_at(receipts) + 4,
      metadata_refs: %{
        projection_ref: projection.projection_ref,
        operation_context_ref: projection.operation_context_ref,
        lower_receipt_summary_ref: "lower-receipt-summary://#{projection.operation_context_ref}"
      }
    )
  end

  defp replay_exported_event(projection, receipts, projection_event_ref) do
    event(
      projection,
      :replay_exported,
      10_001,
      predecessor_event_refs: [projection_event_ref],
      occurred_at: last_completed_at(receipts) + 5,
      metadata_refs: %{
        projection_ref: projection.projection_ref,
        replay_export_ref: "replay-export://#{projection.projection_ref}"
      }
    )
  end

  defp projection_updated_event(projection, receipts, operation_events, causal_order) do
    predecessors =
      operation_events
      |> Enum.filter(&(&1.event_kind == :receipt_reduced))
      |> Enum.map(& &1.event_ref)

    event(
      projection,
      :projection_updated,
      causal_order,
      predecessor_event_refs: predecessors,
      projection_visible?: true,
      projection_key: projection.projection_ref,
      occurred_at: last_completed_at(receipts) + 2,
      metadata_refs: %{
        projection_ref: projection.projection_ref,
        operation_context_ref: projection.operation_context_ref,
        lower_receipt_summary_ref: "lower-receipt-summary://#{projection.operation_context_ref}"
      }
    )
  end

  defp event(projection, kind, causal_order, opts) do
    receipt = Keyword.get(opts, :operation_receipt)
    metadata_refs = Keyword.get(opts, :metadata_refs, %{})
    predecessor_event_refs = Keyword.get(opts, :predecessor_event_refs, [])
    projection_visible? = Keyword.get(opts, :projection_visible?, false)

    event_ref = event_ref(projection, receipt, kind, causal_order)

    %{
      event_ref: event_ref,
      trace_ref: trace_ref(projection, receipt),
      event_kind: kind,
      occurred_at: Keyword.get(opts, :occurred_at, receipt_started_at(receipt)),
      predecessor_event_refs: predecessor_event_refs,
      root_event?: Keyword.get(opts, :root_event?, false),
      projection_key: Keyword.get(opts, :projection_key),
      projection_visible?: projection_visible?,
      projection_order_key: projection_order_key(causal_order, event_ref),
      causal_order: causal_order,
      merge_semantics: merge_semantics(kind, projection_visible?),
      trace_level: @trace_level,
      metadata_refs: metadata_refs
    }
  end

  defp operation_metadata_refs(%OperationReceipt{} = receipt) do
    metadata = receipt.metadata || %{}

    %{
      receipt_ref: receipt.receipt_ref,
      operation_context_ref: receipt.operation_context_ref,
      operation_plan_ref: receipt.operation_plan_ref,
      result_ref: result_ref(receipt.result),
      payload_ref: payload_ref(metadata),
      connector_manifest_ref: metadata_value(metadata, :connector_manifest_ref),
      credential_lease_ref: metadata_value(metadata, :credential_lease_ref),
      effect_request_ref: metadata_value(metadata, :effect_request_ref),
      evidence_ref: metadata_value(metadata, :evidence_ref)
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp evidence_metadata_refs(%OperationReceipt{} = receipt) do
    metadata_refs = operation_metadata_refs(receipt)

    case metadata_value(receipt.metadata || %{}, :evidence_ref) do
      nil -> metadata_refs
      evidence_ref -> Map.put(metadata_refs, :evidence_ref, evidence_ref)
    end
  end

  defp operation_terminal_event_refs(events) do
    events
    |> Enum.filter(&(&1.event_kind in [:receipt_reduced, :evidence_attached]))
    |> Enum.map(& &1.event_ref)
  end

  defp review_event_kind(:opened), do: :review_opened
  defp review_event_kind("opened"), do: :review_opened
  defp review_event_kind(_state), do: :review_skipped

  defp evidence_operation?(%OperationReceipt{} = receipt) do
    metadata_value(receipt.metadata || %{}, :operation_role) == :evidence
  end

  defp event_ref(projection, nil, kind, causal_order) do
    hash = hash_ref("#{projection.projection_ref}:#{kind}:#{causal_order}")
    "lineage://#{kind}/#{hash}"
  end

  defp event_ref(projection, receipt, kind, causal_order) do
    hash = hash_ref("#{projection.projection_ref}:#{receipt.receipt_ref}:#{kind}:#{causal_order}")
    "lineage://#{kind}/#{hash}"
  end

  defp projection_order_key(causal_order, event_ref) do
    causal_order
    |> Integer.to_string()
    |> String.pad_leading(8, "0")
    |> Kernel.<>(":#{event_ref}")
  end

  defp merge_semantics(:projection_updated, true), do: :last_write_by_causal_order
  defp merge_semantics(:receipt_reduced, _visible?), do: :append_by_projection_order
  defp merge_semantics(_kind, _visible?), do: :set_union

  defp trace_ref(projection), do: trace_ref(projection, nil)
  defp trace_ref(_projection, %OperationReceipt{} = receipt), do: receipt.trace_ref

  defp trace_ref(projection, _receipt) do
    projection.operations
    |> List.first()
    |> case do
      %{trace_ref: trace_ref} when is_binary(trace_ref) -> trace_ref
      _missing -> "trace://unknown"
    end
  end

  defp result_ref(%ResultEnvelope{} = result), do: result.result_ref
  defp result_ref(_result), do: nil

  defp payload_ref(metadata) do
    case metadata_value(metadata, :payload_envelope) || metadata_value(metadata, :payload) do
      %PayloadEnvelope{} = payload -> payload.payload_ref
      _other -> nil
    end
  end

  defp first_started_at([]), do: 0

  defp first_started_at(receipts) do
    receipts
    |> Enum.map(&receipt_started_at/1)
    |> Enum.min()
  end

  defp last_completed_at([]), do: 0

  defp last_completed_at(receipts) do
    receipts
    |> Enum.map(&completed_at/1)
    |> Enum.max()
  end

  defp receipt_started_at(%OperationReceipt{started_at: %DateTime{} = started_at}) do
    DateTime.to_unix(started_at, :millisecond)
  end

  defp receipt_started_at(_receipt), do: 0

  defp completed_at(%OperationReceipt{completed_at: %DateTime{} = completed_at}) do
    DateTime.to_unix(completed_at, :millisecond)
  end

  defp completed_at(receipt), do: receipt_started_at(receipt)

  defp hash_ref(value) do
    :crypto.hash(:sha256, value)
    |> Base.encode16(case: :lower)
  end

  defp metadata_value(%{} = metadata, key) do
    case Map.fetch(metadata, key) do
      {:ok, value} -> value
      :error -> Map.get(metadata, Atom.to_string(key))
    end
  end
end
