defmodule Mezzanine.ExecutionReceiptWorkerTest do
  use Mezzanine.LifecycleEngine.DataCase, async: false

  alias Mezzanine.Audit.{AuditFact, ExecutionLineageStore}
  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.ExecutionReceiptWorker
  alias Mezzanine.JoinAdvanceWorker
  alias Mezzanine.ParallelBarrier

  alias Mezzanine.Pack.{
    Compiler,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    ProjectionSpec,
    Serializer,
    SubjectKindSpec
  }

  test "perform records successful outcomes and enriches lineage artifacts" do
    installation = active_installation_fixture()

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:receipt-worker:success",
        subject_kind: "expense_request",
        lifecycle_state: "processing",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-receipt-success",
        causation_id: "cause-receipt-success"
      })

    assert {:ok, execution} =
             dispatch_execution(
               subject,
               installation.id,
               "trace-receipt-success",
               "receipt-success"
             )

    assert {:ok, awaiting_receipt_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-1"},
               lower_receipt: %{"state" => "accepted", "run_id" => "run-1"},
               trace_id: execution.trace_id,
               causation_id: "cause-accepted",
               actor_ref: %{kind: :dispatcher}
             })

    outcome = %{
      "receipt_id" => "receipt-1",
      "status" => "ok",
      "lower_receipt" => %{
        "state" => "completed",
        "run_id" => "run-1",
        "attempt_id" => "attempt-1"
      },
      "normalized_outcome" => %{"summary" => "done"},
      "artifact_refs" => ["artifact://transcript-1"],
      "observed_at" => "2026-04-16T04:00:00Z"
    }

    assert :ok = perform_receipt(awaiting_receipt_execution.id, outcome)

    assert {:ok, completed_execution} = Ash.get(ExecutionRecord, awaiting_receipt_execution.id)
    assert completed_execution.dispatch_state == :completed
    assert completed_execution.lower_receipt["state"] == "completed"
    assert %{lifecycle_state: "paid"} = fetch_subject(subject.id)

    assert {:ok, lineage} = ExecutionLineageStore.fetch(awaiting_receipt_execution.id)
    assert lineage.lower_run_id == "run-1"
    assert lineage.lower_attempt_id == "attempt-1"
    assert lineage.artifact_refs == ["artifact://transcript-1"]
  end

  test "perform records failed outcomes with normalized failure payload" do
    installation = active_installation_fixture(execution_failure_transition?: true)

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:receipt-worker:failure",
        subject_kind: "expense_request",
        lifecycle_state: "processing",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-receipt-failure",
        causation_id: "cause-receipt-failure"
      })

    assert {:ok, execution} =
             dispatch_execution(
               subject,
               installation.id,
               "trace-receipt-failure",
               "receipt-failure"
             )

    assert {:ok, awaiting_receipt_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-2"},
               lower_receipt: %{"state" => "accepted", "run_id" => "run-2"},
               trace_id: execution.trace_id,
               causation_id: "cause-accepted-failure",
               actor_ref: %{kind: :dispatcher}
             })

    outcome = %{
      "receipt_id" => "receipt-2",
      "status" => "error",
      "failure_kind" => "semantic_failure",
      "lower_receipt" => %{
        "state" => "failed",
        "run_id" => "run-2",
        "attempt_id" => "attempt-2"
      },
      "normalized_outcome" => %{
        "error" => %{"kind" => "semantic_failure", "reason" => "bad_patch"}
      },
      "observed_at" => "2026-04-16T04:01:00Z"
    }

    assert :ok = perform_receipt(awaiting_receipt_execution.id, outcome)

    assert {:ok, failed_execution} = Ash.get(ExecutionRecord, awaiting_receipt_execution.id)
    assert failed_execution.dispatch_state == :failed
    assert failed_execution.failure_kind == :semantic_failure
    assert failed_execution.last_dispatch_error_kind == "execution_failed"
    assert %{lifecycle_state: "needs_correction"} = fetch_subject(subject.id)

    assert failed_execution.last_dispatch_error_payload == %{
             "error" => %{"kind" => "semantic_failure", "reason" => "bad_patch"}
           }
  end

  test "perform routes frozen inference failure kinds through deterministic lifecycle transitions" do
    for {failure_kind, expected_state} <- [
          {"semantic_failure", "needs_correction"},
          {"timeout", "needs_review"},
          {"infrastructure_error", "needs_review"},
          {"auth_error", "needs_review"},
          {"fatal_error", "needs_review"}
        ] do
      installation =
        active_installation_fixture(
          runtime_class: :inference,
          execution_failure_transition?: true,
          generic_failure_transition?: true
        )

      subject =
        subject_fixture(%{
          installation_id: installation.id,
          source_ref: "expense:receipt-worker:inference-#{failure_kind}",
          subject_kind: "expense_request",
          lifecycle_state: "processing",
          payload: %{"amount_cents" => 12_500},
          trace_id: "trace-receipt-inference-#{failure_kind}",
          causation_id: "cause-receipt-inference-#{failure_kind}"
        })

      assert {:ok, execution} =
               dispatch_execution(
                 subject,
                 installation.id,
                 "trace-receipt-inference-#{failure_kind}",
                 "receipt-inference-#{failure_kind}",
                 dispatch_envelope: %{
                   "capability" => "sandbox.exec",
                   "runtime_class" => "inference"
                 },
                 intent_snapshot: %{"runtime_class" => "inference"}
               )

      assert {:ok, awaiting_receipt_execution} =
               ExecutionRecord.record_accepted(execution, %{
                 submission_ref: %{"id" => "sub-inference-#{failure_kind}"},
                 lower_receipt: %{
                   "state" => "accepted",
                   "run_id" => "run-inference-#{failure_kind}"
                 },
                 trace_id: execution.trace_id,
                 causation_id: "cause-accepted-inference-#{failure_kind}",
                 actor_ref: %{kind: :dispatcher}
               })

      outcome = %{
        "receipt_id" => "receipt-inference-#{failure_kind}",
        "status" => "error",
        "failure_kind" => failure_kind,
        "lower_receipt" => %{
          "state" => "failed",
          "run_id" => "run-inference-#{failure_kind}",
          "attempt_id" => "attempt-inference-#{failure_kind}"
        },
        "normalized_outcome" => %{
          "error" => %{"kind" => failure_kind, "reason" => "fixture-#{failure_kind}"}
        },
        "observed_at" => "2026-04-16T04:05:00Z"
      }

      assert :ok = perform_receipt(awaiting_receipt_execution.id, outcome)

      assert {:ok, failed_execution} = Ash.get(ExecutionRecord, awaiting_receipt_execution.id)
      assert failed_execution.dispatch_state == :failed
      assert failed_execution.failure_kind == String.to_existing_atom(failure_kind)
      assert %{lifecycle_state: ^expected_state} = fetch_subject(subject.id)
    end
  end

  test "perform records late receipts as audit-only when the subject was cancelled" do
    telemetry_ids = attach_telemetry([[:mezzanine, :receipt, :post_cancel]])
    installation = active_installation_fixture()

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:receipt-worker:cancelled-late",
        subject_kind: "expense_request",
        lifecycle_state: "processing",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-receipt-cancelled-late",
        causation_id: "cause-receipt-cancelled-late"
      })

    assert {:ok, execution} =
             dispatch_execution(
               subject,
               installation.id,
               "trace-receipt-cancelled-late",
               "receipt-cancelled-late"
             )

    assert {:ok, awaiting_receipt_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-cancelled-late"},
               lower_receipt: %{"state" => "accepted", "run_id" => "run-cancelled-late"},
               trace_id: execution.trace_id,
               causation_id: "cause-accepted-cancelled-late",
               actor_ref: %{kind: :dispatcher}
             })

    cancel_subject!(subject.id)

    outcome = %{
      "receipt_id" => "receipt-cancelled-late-1",
      "status" => "ok",
      "lower_receipt" => %{
        "state" => "completed",
        "run_id" => "run-cancelled-late"
      },
      "normalized_outcome" => %{"summary" => "completed after cancel"},
      "observed_at" => "2026-04-16T04:01:30Z"
    }

    try do
      assert :ok = perform_receipt(awaiting_receipt_execution.id, outcome)

      assert_receive {:telemetry_event, [:mezzanine, :receipt, :post_cancel], %{count: 1},
                      metadata}

      assert metadata.event_name == "receipt.post_cancel"
      assert metadata.execution_id == awaiting_receipt_execution.id
      assert metadata.subject_id == subject.id
      assert metadata.status == :ok

      assert {:ok, reloaded_execution} = Ash.get(ExecutionRecord, awaiting_receipt_execution.id)
      assert reloaded_execution.dispatch_state == :awaiting_receipt
      assert %{lifecycle_state: "processing"} = fetch_subject(subject.id)

      assert {:ok, audit_facts} =
               AuditFact.list_trace(installation.id, "trace-receipt-cancelled-late")

      audit_kinds = Enum.map(audit_facts, & &1.fact_kind)
      assert :post_cancel_receipt in audit_kinds
      assert :reconciliation_warning in audit_kinds
    after
      detach_telemetry(telemetry_ids)
    end
  end

  test "perform normalizes missing required lifecycle hints to semantic failure" do
    installation = active_installation_fixture(execution_failure_transition?: true)

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:receipt-worker:missing-hint",
        subject_kind: "expense_request",
        lifecycle_state: "processing",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-receipt-missing-hint",
        causation_id: "cause-receipt-missing-hint"
      })

    assert {:ok, execution} =
             dispatch_execution(
               subject,
               installation.id,
               "trace-receipt-missing-hint",
               "receipt-missing-hint",
               intent_snapshot: %{"required_lifecycle_hints" => ["ticket_status"]}
             )

    assert {:ok, awaiting_receipt_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-missing-hint"},
               lower_receipt: %{"state" => "accepted", "run_id" => "run-missing-hint"},
               trace_id: execution.trace_id,
               causation_id: "cause-accepted-missing-hint",
               actor_ref: %{kind: :dispatcher}
             })

    outcome = %{
      "receipt_id" => "receipt-missing-hint-1",
      "status" => "ok",
      "lower_receipt" => %{
        "state" => "completed",
        "run_id" => "run-missing-hint",
        "attempt_id" => "attempt-missing-hint"
      },
      "normalized_outcome" => %{"summary" => "completed without typed hints"},
      "observed_at" => "2026-04-16T04:01:00Z"
    }

    assert :ok = perform_receipt(awaiting_receipt_execution.id, outcome)

    assert {:ok, failed_execution} = Ash.get(ExecutionRecord, awaiting_receipt_execution.id)
    assert failed_execution.dispatch_state == :failed
    assert failed_execution.failure_kind == :semantic_failure

    assert failed_execution.last_dispatch_error_payload == %{
             "missing_keys" => ["ticket_status"],
             "reason" => "missing_required_hint",
             "reported_status" => "ok"
           }

    assert %{lifecycle_state: "needs_correction"} = fetch_subject(subject.id)
  end

  test "perform creates a fresh linked execution row for semantic retries" do
    installation =
      active_installation_fixture(
        execution_failure_transition?: true,
        semantic_retry_transition?: true
      )

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:receipt-worker:semantic-retry",
        subject_kind: "expense_request",
        lifecycle_state: "processing",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-receipt-semantic-retry",
        causation_id: "cause-receipt-semantic-retry"
      })

    assert {:ok, execution} =
             dispatch_execution(
               subject,
               installation.id,
               "trace-receipt-semantic-retry",
               "receipt-semantic-retry"
             )

    assert {:ok, awaiting_receipt_execution} =
             ExecutionRecord.record_accepted(execution, %{
               submission_ref: %{"id" => "sub-3"},
               lower_receipt: %{"state" => "accepted", "run_id" => "run-3"},
               trace_id: execution.trace_id,
               causation_id: "cause-accepted-semantic-retry",
               actor_ref: %{kind: :dispatcher}
             })

    outcome = %{
      "receipt_id" => "receipt-3",
      "status" => "error",
      "failure_kind" => "semantic_failure",
      "lower_receipt" => %{
        "state" => "failed",
        "run_id" => "run-3",
        "attempt_id" => "attempt-3"
      },
      "normalized_outcome" => %{
        "error" => %{"kind" => "semantic_failure", "reason" => "needs_replan"}
      },
      "observed_at" => "2026-04-16T04:02:00Z"
    }

    assert :ok = perform_receipt(awaiting_receipt_execution.id, outcome)

    assert {:ok, failed_execution} = Ash.get(ExecutionRecord, awaiting_receipt_execution.id)
    assert failed_execution.dispatch_state == :failed

    retry_execution =
      subject_execution_ids(subject.id)
      |> Enum.reject(&(&1 == failed_execution.id))
      |> Enum.map(&Ash.get!(ExecutionRecord, &1))
      |> List.first()

    assert retry_execution.dispatch_state == :pending_dispatch
    assert retry_execution.supersedes_execution_id == failed_execution.id
    assert retry_execution.supersession_reason == :retry_semantic
    assert retry_execution.supersession_depth == failed_execution.supersession_depth + 1
    assert retry_execution.submission_dedupe_key != failed_execution.submission_dedupe_key
    assert %{lifecycle_state: "processing"} = fetch_subject(subject.id)
  end

  test "barrier-bound terminal completions enqueue one join advance and wait for the explicit join trigger" do
    telemetry_ids =
      attach_telemetry([
        [:mezzanine, :join, :advance, :enqueued],
        [:mezzanine, :barrier, :close],
        [:mezzanine, :join, :advance, :idempotent_drop]
      ])

    installation = active_installation_fixture(join_transition?: true)

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:receipt-worker:join",
        subject_kind: "expense_request",
        lifecycle_state: "awaiting_join",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-receipt-join",
        causation_id: "cause-receipt-join"
      })

    assert {:ok, barrier} =
             ParallelBarrier.open(%{
               subject_id: subject.id,
               barrier_key: "fanout:receipt-worker",
               join_step_ref: "triage_join",
               expected_children: 2,
               trace_id: "trace-receipt-join"
             })

    assert {:ok, first_execution} =
             dispatch_execution(
               subject,
               installation.id,
               "trace-receipt-join",
               "receipt-join-1",
               barrier_id: barrier.id
             )

    assert {:ok, second_execution} =
             dispatch_execution(
               subject,
               installation.id,
               "trace-receipt-join",
               "receipt-join-2",
               barrier_id: barrier.id
             )

    assert {:ok, awaiting_first_execution} =
             ExecutionRecord.record_accepted(first_execution, %{
               submission_ref: %{"id" => "sub-join-1"},
               lower_receipt: %{"state" => "accepted", "run_id" => "run-join-1"},
               trace_id: first_execution.trace_id,
               causation_id: "cause-accepted-join-1",
               actor_ref: %{kind: :dispatcher}
             })

    assert {:ok, awaiting_second_execution} =
             ExecutionRecord.record_accepted(second_execution, %{
               submission_ref: %{"id" => "sub-join-2"},
               lower_receipt: %{"state" => "accepted", "run_id" => "run-join-2"},
               trace_id: second_execution.trace_id,
               causation_id: "cause-accepted-join-2",
               actor_ref: %{kind: :dispatcher}
             })

    first_outcome = %{
      "receipt_id" => "receipt-join-1",
      "status" => "ok",
      "lower_receipt" => %{
        "state" => "completed",
        "run_id" => "run-join-1",
        "attempt_id" => "attempt-join-1"
      },
      "normalized_outcome" => %{"summary" => "child one done"},
      "observed_at" => "2026-04-17T06:05:00Z"
    }

    second_outcome = %{
      "receipt_id" => "receipt-join-2",
      "status" => "ok",
      "lower_receipt" => %{
        "state" => "completed",
        "run_id" => "run-join-2",
        "attempt_id" => "attempt-join-2"
      },
      "normalized_outcome" => %{"summary" => "child two done"},
      "observed_at" => "2026-04-17T06:06:00Z"
    }

    try do
      assert :ok = perform_receipt(awaiting_first_execution.id, first_outcome)
      assert %{lifecycle_state: "awaiting_join"} = fetch_subject(subject.id)
      assert {:ok, barrier_after_first} = ParallelBarrier.fetch(barrier.id)
      assert barrier_after_first.status == :open
      assert barrier_after_first.completed_children == 1
      assert join_job_ids_for(subject.id, barrier.id) == []

      assert :ok = perform_receipt(awaiting_second_execution.id, second_outcome)

      assert_receive {:telemetry_event, [:mezzanine, :join, :advance, :enqueued], %{count: 1},
                      enqueued_metadata}

      assert enqueued_metadata.event_name == "join.advance.enqueued"
      assert enqueued_metadata.execution_id == awaiting_second_execution.id
      assert enqueued_metadata.barrier_id == barrier.id
      assert enqueued_metadata.subject_id == subject.id

      assert %{lifecycle_state: "awaiting_join"} = fetch_subject(subject.id)
      assert {:ok, barrier_after_second} = ParallelBarrier.fetch(barrier.id)
      assert barrier_after_second.status == :ready
      assert barrier_after_second.completed_children == 2
      assert join_job_ids_for(subject.id, barrier.id) |> length() == 1

      assert :ok = perform_receipt(awaiting_second_execution.id, second_outcome)
      assert join_job_ids_for(subject.id, barrier.id) |> length() == 1

      assert :ok = perform_join_advance(subject.id, barrier.id)

      assert_receive {:telemetry_event, [:mezzanine, :barrier, :close],
                      %{count: 1, contention_ms: _}, close_metadata}

      assert close_metadata.event_name == "barrier.close"
      assert close_metadata.barrier_id == barrier.id
      assert close_metadata.subject_id == subject.id
      assert close_metadata.barrier_status == :closed

      assert %{lifecycle_state: "paid"} = fetch_subject(subject.id)
      assert {:ok, closed_barrier} = ParallelBarrier.fetch(barrier.id)
      assert closed_barrier.status == :closed

      assert :ok = perform_join_advance(subject.id, barrier.id)

      assert_receive {:telemetry_event, [:mezzanine, :join, :advance, :idempotent_drop],
                      %{count: 1}, drop_metadata}

      assert drop_metadata.event_name == "join.advance.idempotent_drop"
      assert drop_metadata.barrier_id == barrier.id
      assert drop_metadata.subject_id == subject.id
      assert %{lifecycle_state: "paid"} = fetch_subject(subject.id)
    after
      detach_telemetry(telemetry_ids)
    end
  end

  defp active_installation_fixture(opts \\ []) do
    compiled_pack = fixture_compiled_pack(opts)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    registration_id = Ecto.UUID.generate()
    installation_id = Ecto.UUID.generate()
    environment = Keyword.get(opts, :environment, "stage9-#{System.unique_integer([:positive])}")

    binding_config =
      Keyword.get(
        opts,
        :binding_config,
        %{
          "execution_bindings" => %{
            "expense_capture" => %{
              "placement_ref" => "local_runner",
              "execution_params" => %{"timeout_ms" => 300_000}
            }
          }
        }
      )

    Repo.query!(
      """
      INSERT INTO pack_registrations (
        id,
        status,
        version,
        inserted_at,
        updated_at,
        compiled_manifest,
        pack_slug,
        canonical_subject_kinds,
        serializer_version,
        migration_strategy
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      """,
      [
        dump_uuid!(registration_id),
        "active",
        compiled_pack.version,
        now,
        now,
        Serializer.serialize_compiled(compiled_pack),
        to_string(compiled_pack.pack_slug),
        Map.keys(compiled_pack.subject_kinds),
        1,
        "additive"
      ]
    )

    Repo.query!(
      """
      INSERT INTO installations (
        id,
        status,
        metadata,
        inserted_at,
        updated_at,
        compiled_pack_revision,
        tenant_id,
        binding_config,
        pack_slug,
        environment,
        pack_registration_id
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      """,
      [
        dump_uuid!(installation_id),
        "active",
        %{},
        now,
        now,
        1,
        "tenant-lifecycle-engine",
        binding_config,
        to_string(compiled_pack.pack_slug),
        environment,
        dump_uuid!(registration_id)
      ]
    )

    %{id: installation_id, compiled_pack_revision: 1}
  end

  defp fixture_compiled_pack(opts) do
    execution_failure_transition? = Keyword.get(opts, :execution_failure_transition?, false)
    semantic_retry_transition? = Keyword.get(opts, :semantic_retry_transition?, false)
    join_transition? = Keyword.get(opts, :join_transition?, false)
    generic_failure_transition? = Keyword.get(opts, :generic_failure_transition?, false)
    runtime_class = Keyword.get(opts, :runtime_class, :session)
    version = Keyword.get(opts, :version, "1.0.#{System.unique_integer([:positive])}")

    transitions = [
      %{from: :submitted, to: :processing, trigger: {:execution_requested, :expense_capture}},
      %{from: :processing, to: :paid, trigger: {:execution_completed, :expense_capture}}
    ]

    transitions =
      if execution_failure_transition? do
        transitions =
          transitions ++
            [
              %{
                from: :processing,
                to: :needs_correction,
                trigger: {:execution_failed, :expense_capture, :semantic_failure}
              }
            ]

        if semantic_retry_transition? do
          transitions ++
            [
              %{
                from: :needs_correction,
                to: :processing,
                trigger: {:execution_requested, :expense_capture}
              }
            ]
        else
          transitions
        end
      else
        transitions
      end

    transitions =
      if generic_failure_transition? do
        transitions ++
          [
            %{
              from: :processing,
              to: :needs_review,
              trigger: {:execution_failed, :expense_capture}
            }
          ]
      else
        transitions
      end

    transitions =
      if join_transition? do
        transitions ++
          [
            %{from: :awaiting_join, to: :paid, trigger: {:join_completed, :triage_join}}
          ]
      else
        transitions
      end

    manifest = %Manifest{
      pack_slug: :expense_approval,
      version: version,
      subject_kind_specs: [%SubjectKindSpec{name: :expense_request}],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :expense_request,
          initial_state: :submitted,
          terminal_states:
            terminal_states(
              execution_failure_transition?,
              semantic_retry_transition?,
              generic_failure_transition?
            ),
          transitions: transitions
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: :expense_capture,
          runtime_class: runtime_class,
          placement_ref: :local_runner,
          execution_params: %{timeout_ms: 300_000},
          retry_config: %{
            max_attempts: 3,
            backoff: :exponential,
            rekey_on: if(execution_failure_transition?, do: [:semantic_failure], else: [])
          }
        }
      ],
      projection_specs: [
        %ProjectionSpec{name: :active_expenses, subject_kinds: [:expense_request]}
      ]
    }

    {:ok, compiled_pack} = Compiler.compile(manifest)
    compiled_pack
  end

  defp subject_execution_ids(subject_id) do
    %{rows: rows} =
      Repo.query!(
        """
        SELECT id
        FROM execution_records
        WHERE subject_id = $1::uuid
        ORDER BY inserted_at ASC
        """,
        [dump_uuid!(subject_id)]
      )

    Enum.map(rows, fn [id] -> load_uuid!(id) end)
  end

  defp perform_receipt(execution_id, outcome) do
    ExecutionReceiptWorker.perform(%Oban.Job{
      id: 42,
      attempt: 1,
      queue: "receipt",
      args: %{"execution_id" => execution_id, "outcome" => outcome}
    })
  end

  defp perform_join_advance(subject_id, barrier_id) do
    JoinAdvanceWorker.perform(%Oban.Job{
      id: 43,
      attempt: 1,
      queue: "join",
      args: %{"subject_id" => subject_id, "barrier_id" => barrier_id}
    })
  end

  defp dispatch_execution(subject, installation_id, trace_id, suffix, opts \\ []) do
    ExecutionRecord.dispatch(%{
      tenant_id: installation_id,
      installation_id: installation_id,
      subject_id: subject.id,
      barrier_id: Keyword.get(opts, :barrier_id),
      recipe_ref: "expense_capture",
      compiled_pack_revision: 1,
      binding_snapshot: Keyword.get(opts, :binding_snapshot, %{}),
      dispatch_envelope: Keyword.get(opts, :dispatch_envelope, %{"capability" => "sandbox.exec"}),
      intent_snapshot: Keyword.get(opts, :intent_snapshot, %{}),
      submission_dedupe_key: "#{installation_id}:exec:#{suffix}",
      trace_id: trace_id,
      causation_id: "cause-#{suffix}",
      actor_ref: %{kind: :scheduler}
    })
  end

  defp join_job_ids_for(subject_id, barrier_id) do
    Repo.all(Oban.Job)
    |> Enum.filter(fn job ->
      job.worker == Oban.Worker.to_string(JoinAdvanceWorker) and
        job.args["subject_id"] == subject_id and
        job.args["barrier_id"] == barrier_id
    end)
    |> Enum.map(& &1.id)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp subject_fixture(attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    subject_id = Ecto.UUID.generate()

    Repo.query!(
      """
      INSERT INTO subject_records (
        id,
        payload,
        installation_id,
        source_ref,
        subject_kind,
        lifecycle_state,
        schema_version,
        opened_at,
        row_version,
        inserted_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
      """,
      [
        dump_uuid!(subject_id),
        Map.fetch!(attrs, :payload),
        Map.fetch!(attrs, :installation_id),
        Map.fetch!(attrs, :source_ref),
        Map.fetch!(attrs, :subject_kind),
        Map.fetch!(attrs, :lifecycle_state),
        1,
        now,
        1,
        now,
        now
      ]
    )

    Repo.query!(
      """
      INSERT INTO audit_facts (
        id,
        installation_id,
        subject_id,
        execution_id,
        trace_id,
        causation_id,
        fact_kind,
        actor_ref,
        payload,
        occurred_at,
        inserted_at,
        updated_at
      )
      VALUES ($1, $2, $3, NULL, $4, $5, $6, $7, $8, $9, $10, $11)
      """,
      [
        dump_uuid!(Ecto.UUID.generate()),
        Map.fetch!(attrs, :installation_id),
        subject_id,
        Map.fetch!(attrs, :trace_id),
        Map.fetch!(attrs, :causation_id),
        "subject_ingested",
        %{"kind" => "intake"},
        %{
          "source_ref" => Map.fetch!(attrs, :source_ref),
          "subject_kind" => Map.fetch!(attrs, :subject_kind),
          "lifecycle_state" => Map.fetch!(attrs, :lifecycle_state)
        },
        now,
        now,
        now
      ]
    )

    %{id: subject_id}
  end

  defp fetch_subject(subject_id) do
    %{rows: [[id, lifecycle_state, row_version]]} =
      Repo.query!(
        """
        SELECT id, lifecycle_state, row_version
        FROM subject_records
        WHERE id = $1::uuid
        """,
        [dump_uuid!(subject_id)]
      )

    %{
      id: load_uuid!(id),
      lifecycle_state: lifecycle_state,
      row_version: row_version
    }
  end

  defp terminal_states(
         execution_failure_transition?,
         semantic_retry_transition?,
         generic_failure_transition?
       ) do
    [:paid]
    |> maybe_add_terminal_state(
      execution_failure_transition? and not semantic_retry_transition?,
      :needs_correction
    )
    |> maybe_add_terminal_state(generic_failure_transition?, :needs_review)
  end

  defp maybe_add_terminal_state(states, true, state), do: states ++ [state]
  defp maybe_add_terminal_state(states, false, _state), do: states

  defp cancel_subject!(subject_id) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    Repo.query!(
      """
      UPDATE subject_records
      SET status = 'cancelled',
          status_reason = 'operator cancelled',
          status_updated_at = $2,
          terminal_at = $2,
          updated_at = $2
      WHERE id = $1::uuid
      """,
      [dump_uuid!(subject_id), now]
    )
  end

  defp dump_uuid!(value), do: Ecto.UUID.dump!(value)

  defp load_uuid!(value) when is_binary(value) and byte_size(value) == 16,
    do: Ecto.UUID.load!(value)

  defp load_uuid!(value) when is_binary(value), do: value

  defp attach_telemetry(events) do
    Enum.map(events, fn event ->
      handler_id = {__MODULE__, make_ref(), event}
      :ok = :telemetry.attach(handler_id, event, &__MODULE__.handle_telemetry_event/4, self())
      handler_id
    end)
  end

  defp detach_telemetry(handler_ids) do
    Enum.each(handler_ids, &:telemetry.detach/1)
  end

  def handle_telemetry_event(event, measurements, metadata, pid) do
    send(pid, {:telemetry_event, event, measurements, metadata})
  end
end
