defmodule Mezzanine.Projections.OperationReceiptSummary do
  @moduledoc "Generic operation-list summary used by subject runtime projections."

  alias Mezzanine.Projections.EnvelopeAccessSummary
  alias Mezzanine.Substrate.{OperationReceipt, PayloadEnvelope, ResultEnvelope}

  @enforce_keys [
    :receipt_ref,
    :operation_context_ref,
    :operation_plan_ref,
    :trace_ref,
    :status,
    :result_ref
  ]
  defstruct @enforce_keys ++
              [
                :operation_role,
                :operation_class,
                :started_at,
                :completed_at,
                :payload_access,
                :result_access,
                :result_schema_ref,
                lineage_event_refs: [],
                provider_object_refs: %{},
                metadata: %{}
              ]

  @type t :: %__MODULE__{}

  @spec from_receipt(OperationReceipt.t()) :: t()
  def from_receipt(%OperationReceipt{} = receipt) do
    metadata = receipt.metadata || %{}

    %__MODULE__{
      receipt_ref: receipt.receipt_ref,
      operation_context_ref: receipt.operation_context_ref,
      operation_plan_ref: receipt.operation_plan_ref,
      trace_ref: receipt.trace_ref,
      status: status_atom(receipt.status),
      result_ref: result_ref(receipt.result),
      operation_role: metadata_value(metadata, :operation_role),
      operation_class: metadata_value(metadata, :operation_class),
      started_at: receipt.started_at,
      completed_at: receipt.completed_at,
      payload_access: payload_access(metadata),
      result_access: result_access(receipt.result),
      result_schema_ref: result_schema_ref(receipt.result),
      lineage_event_refs: receipt.lineage_event_refs,
      provider_object_refs: metadata_value(metadata, :provider_object_refs) || %{},
      metadata:
        Map.drop(metadata, [
          :provider_object_refs,
          :provider_facts,
          :extensions,
          "provider_object_refs",
          "provider_facts",
          "extensions",
          :payload,
          :payload_envelope,
          "payload",
          "payload_envelope"
        ])
    }
  end

  defp result_ref(%ResultEnvelope{} = result), do: result.result_ref
  defp result_ref(%{} = result), do: metadata_value(result, :result_ref)
  defp result_ref(_result), do: nil

  defp result_access(%ResultEnvelope{} = result), do: EnvelopeAccessSummary.from_result(result)
  defp result_access(_result), do: nil

  defp payload_access(metadata) do
    case metadata_value(metadata, :payload_envelope) || metadata_value(metadata, :payload) do
      %PayloadEnvelope{} = payload -> EnvelopeAccessSummary.from_payload(payload)
      _other -> nil
    end
  end

  defp result_schema_ref(%ResultEnvelope{} = result), do: result.schema_ref
  defp result_schema_ref(%{} = result), do: metadata_value(result, :schema_ref)
  defp result_schema_ref(_result), do: nil

  defp status_atom(value) when is_atom(value), do: value
  defp status_atom("accepted"), do: :accepted
  defp status_atom("completed"), do: :completed
  defp status_atom("succeeded"), do: :succeeded
  defp status_atom("failed"), do: :failed
  defp status_atom("terminal_failure"), do: :terminal_failure
  defp status_atom("retryable"), do: :retryable
  defp status_atom("retryable_failure"), do: :retryable_failure
  defp status_atom("canceled"), do: :canceled
  defp status_atom("cancelled"), do: :cancelled
  defp status_atom("blocked"), do: :blocked
  defp status_atom("input_required"), do: :input_required
  defp status_atom("partial_success"), do: :partial_success
  defp status_atom(_value), do: :unknown

  defp metadata_value(%{} = metadata, key) do
    case Map.fetch(metadata, key) do
      {:ok, value} -> value
      :error -> Map.get(metadata, Atom.to_string(key))
    end
  end
end

defmodule Mezzanine.Projections.LowerReceiptSummary do
  @moduledoc "Generic lower receipt summary made from operation and group lists."

  @enforce_keys [:summary_ref, :status]
  defstruct @enforce_keys ++
              [
                operations: [],
                operation_groups: [],
                provider_object_refs: %{},
                metadata: %{}
              ]

  @type t :: %__MODULE__{}

  @spec from_projection(map()) :: t()
  def from_projection(projection) do
    %__MODULE__{
      summary_ref: "lower-receipt-summary://#{projection.operation_context_ref}",
      status: projection.status,
      operations: projection.operations,
      operation_groups: projection.operation_groups,
      provider_object_refs: projection.provider_object_refs,
      metadata: %{
        projection_ref: projection.projection_ref,
        subject_ref: projection.subject_ref
      }
    }
  end
end

defmodule Mezzanine.Projections.SubjectRuntimeProjection do
  @moduledoc "Generic subject runtime projection built from operation receipts."

  alias Mezzanine.Projections.OperationReceiptSummary
  alias Mezzanine.Substrate.{OperationGroupReceipt, OperationReceipt}

  @enforce_keys [:projection_ref, :operation_context_ref, :subject_ref, :status]
  defstruct @enforce_keys ++
              [
                operations: [],
                operation_groups: [],
                evidence: [],
                source_publications: [],
                resource_effects: [],
                provider_facts: [],
                provider_object_refs: %{},
                extensions: %{},
                lineage_event_refs: [],
                metadata: %{}
              ]

  @type t :: %__MODULE__{}

  @spec from_operation_receipts([OperationReceipt.t()], keyword()) ::
          {:ok, t()} | {:error, term()}
  def from_operation_receipts(receipts, opts \\ [])

  def from_operation_receipts([%OperationReceipt{} | _rest] = receipts, opts)
      when is_list(opts) do
    summaries = Enum.map(receipts, &OperationReceiptSummary.from_receipt/1)

    operation_context_ref =
      Keyword.get(opts, :operation_context_ref) || operation_context_ref!(summaries)

    subject_ref = Keyword.get(opts, :subject_ref) || subject_ref_from_receipts(receipts)

    {:ok,
     %__MODULE__{
       projection_ref: "subject-runtime-projection://#{operation_context_ref}",
       operation_context_ref: operation_context_ref,
       subject_ref: subject_ref,
       status: aggregate_status(summaries),
       operations: summaries,
       evidence: filter_role(summaries, :evidence),
       source_publications: filter_role(summaries, :publication),
       resource_effects: filter_role(summaries, :resource_effect),
       provider_facts: provider_facts(receipts),
       provider_object_refs: provider_object_refs(summaries),
       extensions: extensions(receipts),
       lineage_event_refs: lineage_event_refs(summaries),
       metadata: %{source: :operation_receipts}
     }}
  end

  def from_operation_receipts(_receipts, _opts), do: {:error, :missing_operation_receipts}

  @spec from_operation_group(OperationGroupReceipt.t(), [OperationReceipt.t()], keyword()) ::
          {:ok, t()} | {:error, term()}
  def from_operation_group(%OperationGroupReceipt{} = group, receipts, opts \\ [])
      when is_list(opts) do
    summaries = Enum.map(receipts, &OperationReceiptSummary.from_receipt/1)

    with {:ok, child_operations} <- child_operations(group, summaries) do
      operation_context_ref = group.operation_context_ref

      subject_ref =
        Keyword.get(opts, :subject_ref) || metadata_value(group.metadata || %{}, :subject_ref)

      group_summary = %{
        group_receipt_ref: group.group_receipt_ref,
        operation_context_ref: group.operation_context_ref,
        receipt_refs: group.receipt_refs,
        status: status_atom(group.status),
        group_kind: metadata_value(group.metadata || %{}, :group_kind),
        child_operations: child_operations,
        metadata: group.metadata || %{}
      }

      {:ok,
       %__MODULE__{
         projection_ref: "subject-runtime-projection://#{operation_context_ref}",
         operation_context_ref: operation_context_ref,
         subject_ref: subject_ref,
         status: status_atom(group.status),
         operations: child_operations,
         operation_groups: [group_summary],
         evidence: filter_role(child_operations, :evidence),
         source_publications: filter_role(child_operations, :publication),
         resource_effects: filter_role(child_operations, :resource_effect),
         provider_facts: provider_facts(receipts),
         provider_object_refs: provider_object_refs(child_operations),
         extensions: extensions(receipts),
         lineage_event_refs: lineage_event_refs(child_operations),
         metadata: %{source: :operation_group_receipt}
       }}
    end
  end

  defp child_operations(group, summaries) do
    summaries_by_ref = Map.new(summaries, &{&1.receipt_ref, &1})

    group.receipt_refs
    |> Enum.reduce_while({:ok, []}, fn receipt_ref, {:ok, collected} ->
      case Map.fetch(summaries_by_ref, receipt_ref) do
        {:ok, summary} -> {:cont, {:ok, [summary | collected]}}
        :error -> {:halt, {:error, {:missing_group_receipt_ref, receipt_ref}}}
      end
    end)
    |> case do
      {:ok, summaries} -> {:ok, Enum.reverse(summaries)}
      error -> error
    end
  end

  defp operation_context_ref!([summary | _rest]), do: summary.operation_context_ref

  defp subject_ref_from_receipts(receipts) do
    receipts
    |> Enum.map(&metadata_value(&1.metadata || %{}, :subject_ref))
    |> Enum.find(&present?/1)
  end

  defp aggregate_status(summaries) do
    statuses = Enum.map(summaries, & &1.status)

    cond do
      Enum.all?(statuses, &(&1 in [:accepted, :completed, :succeeded])) -> :succeeded
      Enum.any?(statuses, &(&1 in [:failed, :terminal_failure])) -> :failed
      Enum.any?(statuses, &(&1 in [:retryable_failure, :retryable])) -> :retryable_failure
      Enum.any?(statuses, &(&1 in [:canceled, :cancelled])) -> :cancelled
      true -> :mixed
    end
  end

  defp filter_role(summaries, role) do
    Enum.filter(summaries, &(role_atom(&1.operation_role) == role))
  end

  defp role_atom(value) when is_atom(value), do: value
  defp role_atom("source"), do: :source
  defp role_atom("publication"), do: :publication
  defp role_atom("runtime"), do: :runtime
  defp role_atom("tool"), do: :tool
  defp role_atom("evidence"), do: :evidence
  defp role_atom("resource_effect"), do: :resource_effect
  defp role_atom(_value), do: nil

  defp provider_facts(receipts) do
    receipts
    |> Enum.flat_map(fn receipt ->
      receipt.metadata
      |> metadata_value(:provider_facts)
      |> List.wrap()
    end)
  end

  defp provider_object_refs(summaries) do
    Enum.reduce(summaries, %{}, fn summary, acc ->
      Map.merge(acc, summary.provider_object_refs, fn _key, left, right ->
        (List.wrap(left) ++ List.wrap(right))
        |> Enum.uniq()
        |> Enum.sort()
      end)
    end)
  end

  defp extensions(receipts) do
    Enum.reduce(receipts, %{}, fn receipt, acc ->
      Map.merge(acc, metadata_value(receipt.metadata || %{}, :extensions) || %{})
    end)
  end

  defp lineage_event_refs(summaries) do
    summaries
    |> Enum.flat_map(& &1.lineage_event_refs)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp status_atom(value) when is_atom(value), do: value
  defp status_atom("accepted"), do: :accepted
  defp status_atom("completed"), do: :completed
  defp status_atom("succeeded"), do: :succeeded
  defp status_atom("failed"), do: :failed
  defp status_atom("terminal_failure"), do: :terminal_failure
  defp status_atom("retryable"), do: :retryable
  defp status_atom("retryable_failure"), do: :retryable_failure
  defp status_atom("canceled"), do: :canceled
  defp status_atom("cancelled"), do: :cancelled
  defp status_atom("blocked"), do: :blocked
  defp status_atom("input_required"), do: :input_required
  defp status_atom("partial_success"), do: :partial_success
  defp status_atom(_value), do: :unknown

  defp metadata_value(nil, _key), do: nil

  defp metadata_value(%{} = metadata, key) do
    case Map.fetch(metadata, key) do
      {:ok, value} -> value
      :error -> Map.get(metadata, Atom.to_string(key))
    end
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
