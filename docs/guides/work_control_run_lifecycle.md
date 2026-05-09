# Work Control Run Lifecycle

`Mezzanine.WorkControl` is the public Mezzanine surface for preparing and
starting governed work after a product has entered through AppKit.

## Entry Points

- `Mezzanine.WorkControl.prepare_run_request/2`
- `Mezzanine.WorkControl.start_run_for_subject/3`
- `Mezzanine.WorkControl.control_session_for_work/2`
- `Mezzanine.WorkControl.ensure_control_session/2`

## Prepare A Run Request

`prepare_run_request/2` accepts a tenant id and attributes that identify the
program, work class, source/work object, and runtime metadata.

Expected caller posture:

- AppKit has already admitted the product request.
- Product code has supplied product-safe refs and requested capability ids.
- No lower provider effect is expected from preparation.

Representative attributes:

```elixir
attrs = %{
  program_id: "extravaganza-coding-ops",
  work_class_id: "coding_operations",
  source_ref: "linear://issue/ENG-123",
  subject_ref: "subject://linear/ENG-123",
  trace_id: "trace-123",
  runtime_profile_ref: "runtime-profile://extravaganza/codex/default",
  runtime_profile_kind: :temporal_local,
  lower_runtime_kind: :codex_session,
  requested_capability_ids: ["codex.session.turn"],
  resource_scope_refs: ["workspace://tenant/ENG-123"]
}

{:ok, %{work_object: work_object, plan: plan}} =
  Mezzanine.WorkControl.prepare_run_request("tenant-1", attrs)
```

## Start A Run

`start_run_for_subject/3` ensures the current plan, control session, run
series, active run, and review unit.

```elixir
{:ok, started} =
  Mezzanine.WorkControl.start_run_for_subject(
    "tenant-1",
    work_object.id,
    attrs
  )

started.run
started.review_unit
```

## What This Surface Guarantees

- Work objects and runs are Mezzanine-owned records.
- Runtime profile and lower metadata are preserved on the run path.
- Review setup is created as part of the work-control lifecycle when required.
- Provider effects are not performed here.

## Next Step

The execution record or run metadata feeds the workflow runtime handoff. The
execution lifecycle workflow then compiles authority and submits lower work.
