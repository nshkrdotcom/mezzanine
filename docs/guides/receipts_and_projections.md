# Receipts And Projections

Mezzanine reduces governed lower receipts into owner ledgers and projections.
The canonical reducer is `Mezzanine.Projections.ReceiptReducer`.

## Entry Point

```elixir
{:ok, result} =
  Mezzanine.Projections.ReceiptReducer.reduce(%{
    installation_id: "installation-1",
    subject_id: subject_id,
    execution_id: execution_id,
    trace_id: "trace-123",
    causation_id: "workflow-signal-1",
    receipt_id: "receipt-1",
    receipt_state: "succeeded",
    lower_receipt_ref: "lower-receipt://request/succeeded",
    lower_receipt: lower_receipt,
    required_evidence: []
  })
```

## Reducer Outputs

The reducer returns:

- execution update
- subject update
- review decisions
- evidence records
- runtime projection
- audit entry

## Projection Name

The primary runtime projection is:

```text
operator_subject_runtime
```

It carries lower receipt metadata, runtime profile refs, authority refs,
governance refs, source publication refs, review refs, and evidence refs for
AppKit readback.

## Receipt Metadata

Lower receipt metadata should preserve:

- lower request ref
- lower runtime kind
- capability id
- resource scope refs
- policy bundle refs
- script refs
- package refs
- sandbox profile ref
- attestation requirement ref
- denial refs

## Boundary Rules

The reducer consumes already-carried refs and receipt metadata. It must not:

- discover provider objects by static selector
- read process environment for authority
- fetch raw provider payloads
- expose raw workflow history
- treat projection rows as lower source of truth
