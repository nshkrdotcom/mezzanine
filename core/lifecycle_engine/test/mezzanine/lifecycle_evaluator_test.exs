defmodule Mezzanine.LifecycleEvaluatorTest do
  use Mezzanine.LifecycleEngine.DataCase, async: false

  alias Mezzanine.Execution.ExecutionRecord
  alias Mezzanine.LifecycleEvaluator

  alias Mezzanine.Pack.{
    Compiler,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    ProjectionSpec,
    Serializer,
    SubjectKindSpec
  }

  test "advance/1 moves the subject through an explicit execution request and records Temporal work" do
    installation = active_installation_fixture()

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:lifecycle-evaluator",
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-subject",
        causation_id: "cause-lifecycle-subject"
      })

    assert {:ok, result} = LifecycleEvaluator.advance(subject.id)
    assert result.action == :queued_execution
    assert result.subject_id == subject.id
    assert result.recipe_ref == "expense_capture"
    assert result.to_state == "processing"
    assert is_binary(result.execution_id)
    assert is_binary(result.submission_dedupe_key)
    assert is_binary(result.trace_id)
    assert result.workflow_handoff.provider == :temporal_workflow
    assert result.workflow_handoff.workflow_module == "Mezzanine.Workflows.ExecutionAttempt"
    assert result.workflow_handoff.workflow_runtime_boundary == "Mezzanine.WorkflowRuntime"
    assert result.workflow_handoff.execution_id == result.execution_id
    assert result.workflow_handoff.outbox_id == "workflow-start://#{result.execution_id}"

    assert result.workflow_handoff.command_receipt_ref ==
             "execution-record://#{result.execution_id}/queued"

    assert result.workflow_handoff.workflow_input_ref == "workflow-input://#{result.execution_id}"

    assert result.workflow_handoff.authority_packet_ref ==
             "citadel-authority-request://#{result.execution_id}"

    assert result.workflow_handoff.permission_decision_ref ==
             "citadel-permission-decision://#{result.execution_id}"

    assert result.workflow_handoff.idempotency_key == result.submission_dedupe_key

    assert result.workflow_handoff.release_manifest_ref ==
             "phase4-v6-milestone31-temporal-cutover"

    assert %{lifecycle_state: "processing"} = fetch_subject(subject.id)

    assert {:ok, execution} = Ash.get(ExecutionRecord, result.execution_id)
    assert execution.installation_id == installation.id
    assert execution.subject_id == subject.id
    assert execution.recipe_ref == "expense_capture"
    assert execution.compiled_pack_revision == installation.compiled_pack_revision
    assert execution.dispatch_state == :queued
    assert execution.submission_dedupe_key == result.submission_dedupe_key
    assert execution.trace_id == result.trace_id

    assert %{
             rows: [
               [
                 "tenant-lifecycle-engine",
                 installation_id,
                 subject_id,
                 execution_id,
                 trace_id
               ]
             ]
           } =
             Repo.query!(
               """
               SELECT tenant_id, installation_id, subject_id, execution_id, trace_id
               FROM execution_lineage_records
               WHERE execution_id = $1
               """,
               [result.execution_id]
             )

    assert installation_id == installation.id
    assert subject_id == subject.id
    assert execution_id == result.execution_id
    assert trace_id == result.trace_id

    assert execution.binding_snapshot == %{
             "placement_ref" => "local_runner",
             "execution_params" => %{"timeout_ms" => 300_000},
             "authority_decision_ref" => "authority-decision://fixture/expense_capture",
             "connector_binding_ref" => "connector-binding://expense_system_api",
             "no_credentials_posture_ref" => "no-credentials://fixture/expense_capture",
             "connector_bindings" => %{
               "expense_system" => %{"connector_key" => "expense_system_api"}
             },
             "actor_bindings" => %{},
             "evidence_bindings" => %{}
           }

    assert execution.dispatch_envelope == %{
             "recipe_ref" => "expense_capture",
             "runtime_class" => "session",
             "placement_ref" => "local_runner",
             "execution_params" => %{"timeout_ms" => 300_000},
             "grant_spec" => %{},
             "authority_decision_ref" => "authority-decision://fixture/expense_capture",
             "no_credentials_posture_ref" => "no-credentials://fixture/expense_capture",
             "dispatch_ref_requirements" => %{
               "authority_decision_ref" => "required",
               "connector_binding_ref" => "required",
               "credential_posture_ref" => "credential_lease_or_no_credentials"
             }
           }

    assert execution.intent_snapshot == %{
             "recipe_ref" => "expense_capture",
             "runtime_class" => "session",
             "required_lifecycle_hints" => [],
             "binding_snapshot" => execution.binding_snapshot,
             "dispatch_envelope" => execution.dispatch_envelope
           }

    assert %{
             rows: [
               [
                 outbox_id,
                 workflow_id,
                 idempotency_key,
                 command_receipt_ref,
                 workflow_input_ref,
                 dispatch_state
               ]
             ]
           } =
             Repo.query!(
               """
               SELECT outbox_id, workflow_id, idempotency_key, command_receipt_ref,
                      workflow_input_ref, dispatch_state
               FROM workflow_start_outbox
               WHERE outbox_id = $1
               """,
               [result.workflow_handoff.outbox_id]
             )

    assert outbox_id == result.workflow_handoff.outbox_id
    assert workflow_id == result.workflow_handoff.workflow_id
    assert idempotency_key == result.submission_dedupe_key
    assert command_receipt_ref == result.workflow_handoff.command_receipt_ref
    assert workflow_input_ref == result.workflow_handoff.workflow_input_ref
    assert dispatch_state == "queued"

    assert Repo.aggregate(Oban.Job, :count, :id) == 1

    assert %{
             rows: [
               [
                 job_args,
                 "workflow_start_outbox",
                 "Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker"
               ]
             ]
           } =
             Repo.query!(
               """
               SELECT args, queue, worker
               FROM oban_jobs
               WHERE args->>'outbox_id' = $1
               """,
               [result.workflow_handoff.outbox_id]
             )

    assert job_args["workflow_id"] == result.workflow_handoff.workflow_id
    assert job_args["idempotency_key"] == result.submission_dedupe_key

    assert list_trace_fact_kinds(subject.id, result.trace_id) == [
             "subject_ingested",
             "lifecycle_advanced",
             "execution_dispatched"
           ]
  end

  test "advance/2 rejects stale caller-visible installation revisions before queuing work" do
    installation = active_installation_fixture()
    installation_id = installation.id

    subject =
      subject_fixture(%{
        installation_id: installation_id,
        source_ref: "expense:request:stale-revision",
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-stale-revision",
        causation_id: "cause-lifecycle-stale-revision"
      })

    assert {:error,
            {:stale_installation_revision,
             %{
               installation_id: ^installation_id,
               attempted_revision: 0,
               current_revision: 1
             }}} =
             LifecycleEvaluator.advance(subject.id, expected_installation_revision: 0)

    assert %{lifecycle_state: "submitted"} = fetch_subject(subject.id)
    assert Repo.aggregate(Oban.Job, :count, :id) == 0
  end

  test "advance/1 treats delayed retry executions as active after restart" do
    installation = active_installation_fixture()

    retry_due_at =
      DateTime.utc_now() |> DateTime.add(120, :second) |> DateTime.truncate(:microsecond)

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:delayed-retry-active",
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-delayed-retry-active",
        causation_id: "cause-delayed-retry-active"
      })

    existing_execution =
      active_delayed_execution_fixture(subject, installation, "trace-delayed-retry-active", %{
        next_dispatch_at: retry_due_at,
        submission_dedupe_key: "delayed-retry-active"
      })

    assert {:ok, result} =
             LifecycleEvaluator.advance(
               subject.id,
               now: DateTime.utc_now() |> DateTime.truncate(:microsecond),
               causation_id: "cause-delayed-retry-active:restart"
             )

    assert result.action == :noop
    assert result.reason == :active_execution_present
    assert result.subject_id == subject.id
    assert execution_count(subject.id) == 1
    assert workflow_start_outbox_count() == 0
    assert Repo.aggregate(Oban.Job, :count, :id) == 0
    assert %{lifecycle_state: "submitted"} = fetch_subject(subject.id)

    assert {:ok, reloaded_execution} = Ash.get(ExecutionRecord, existing_execution.id)
    assert reloaded_execution.dispatch_state == :in_flight
    assert reloaded_execution.next_dispatch_at == retry_due_at
  end

  test "advance/1 rejects missing Phase 2 opaque refs before execution and workflow effects" do
    [
      {default_execution_binding([]) |> Map.delete("authority_decision_ref"),
       :missing_authority_decision_ref},
      {default_execution_binding([]) |> Map.delete("connector_binding_ref"),
       :missing_connector_binding_ref},
      {default_execution_binding([]) |> Map.delete("no_credentials_posture_ref"),
       :missing_no_credentials_posture_ref},
      {default_execution_binding([])
       |> Map.delete("no_credentials_posture_ref")
       |> Map.put("credentials_required", true), :missing_credential_lease_ref}
    ]
    |> Enum.with_index(1)
    |> Enum.each(fn {{binding, error}, index} ->
      installation =
        active_installation_fixture(
          binding_config: execution_binding_config(binding),
          environment: "stage9-opaque-ref-#{index}",
          pack_version: "1.0.#{index}"
        )

      subject =
        subject_fixture(%{
          installation_id: installation.id,
          source_ref: "expense:request:opaque-ref-#{error}",
          subject_kind: "expense_request",
          lifecycle_state: "submitted",
          payload: %{"amount_cents" => 12_500},
          trace_id: "trace-lifecycle-opaque-ref-#{error}",
          causation_id: "cause-lifecycle-opaque-ref-#{error}"
        })

      assert {:error, ^error} = LifecycleEvaluator.advance(subject.id)
      assert_no_dispatch_effects(subject.id)
    end)
  end

  test "advance/1 rejects raw credential material before execution and workflow effects" do
    binding =
      default_execution_binding([])
      |> Map.put("api_key", "raw-provider-secret")

    installation = active_installation_fixture(binding_config: execution_binding_config(binding))

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:raw-credential",
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-raw-credential",
        causation_id: "cause-lifecycle-raw-credential"
      })

    assert {:error, :raw_credential_material_forbidden} = LifecycleEvaluator.advance(subject.id)
    assert_no_dispatch_effects(subject.id)
  end

  test "advance/1 rejects tenant/install mismatch when dispatch attrs carry tenant scope" do
    binding =
      default_execution_binding([])
      |> Map.put("tenant_id", "tenant-other")

    installation = active_installation_fixture(binding_config: execution_binding_config(binding))

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:tenant-mismatch",
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-tenant-mismatch",
        causation_id: "cause-lifecycle-tenant-mismatch"
      })

    assert {:error, :tenant_installation_mismatch} = LifecycleEvaluator.advance(subject.id)
    assert_no_dispatch_effects(subject.id)
  end

  test "advance/1 captures required lifecycle hints inside the execution intent snapshot" do
    installation =
      active_installation_fixture(
        required_lifecycle_hints: [:ticket_status],
        produced_lifecycle_hints: [:ticket_status]
      )

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:lifecycle-hints",
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-hints",
        causation_id: "cause-lifecycle-hints"
      })

    assert {:ok, result} = LifecycleEvaluator.advance(subject.id)
    assert {:ok, execution} = Ash.get(ExecutionRecord, result.execution_id)

    assert execution.intent_snapshot["required_lifecycle_hints"] == ["ticket_status"]

    assert execution.binding_snapshot["connector_capability"] == %{
             "capability_id" => "expense.capture",
             "produces_lifecycle_hints" => ["ticket_status"],
             "version" => "2026.04"
           }
  end

  test "advance/1 keeps inference runtime selection explicit on the binding and intent snapshots" do
    installation =
      active_installation_fixture(
        runtime_class: :inference,
        binding_config: %{
          "execution_bindings" => %{
            "expense_capture" => %{
              "placement_ref" => "memory_reasoner",
              "execution_params" => %{
                "timeout_ms" => 120_000,
                "reasoning_tier" => "deliberate"
              },
              "authority_decision_ref" => "authority-decision://fixture/expense_capture",
              "connector_binding_ref" => "connector-binding://expense_system_api",
              "no_credentials_posture_ref" => "no-credentials://fixture/expense_capture",
              "descriptor" => %{
                "attachment" => "mezzanine.execution_recipe",
                "contract" => "authoritative",
                "ownership" => %{
                  "external_system" => "hindsight",
                  "external_system_ref" => "hindsight.primary"
                }
              }
            }
          }
        }
      )

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:inference-runtime",
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-inference-runtime",
        causation_id: "cause-lifecycle-inference-runtime"
      })

    assert {:ok, result} = LifecycleEvaluator.advance(subject.id)
    assert {:ok, execution} = Ash.get(ExecutionRecord, result.execution_id)

    assert execution.binding_snapshot["placement_ref"] == "memory_reasoner"

    assert execution.binding_snapshot["execution_params"] == %{
             "timeout_ms" => 120_000,
             "reasoning_tier" => "deliberate"
           }

    assert get_in(execution.binding_snapshot, ["descriptor", "attachment"]) ==
             "mezzanine.execution_recipe"

    assert get_in(execution.binding_snapshot, ["descriptor", "ownership", "external_system"]) ==
             "hindsight"

    assert get_in(
             execution.binding_snapshot,
             ["descriptor", "ownership", "external_system_ref"]
           ) == "hindsight.primary"

    assert execution.dispatch_envelope["runtime_class"] == "inference"
    assert execution.intent_snapshot["runtime_class"] == "inference"
    assert execution.intent_snapshot["binding_snapshot"] == execution.binding_snapshot
    assert execution.intent_snapshot["dispatch_envelope"] == execution.dispatch_envelope
  end

  test "advance/1 refuses to infer executable work when no explicit execution request exists" do
    installation = active_installation_fixture(no_execution_request?: true)

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:no-execution-request",
        subject_kind: "expense_request",
        lifecycle_state: "submitted",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-noop",
        causation_id: "cause-lifecycle-noop"
      })

    assert {:ok, %{action: :noop, reason: :no_execution_requested_transition}} =
             LifecycleEvaluator.advance(subject.id)

    assert %{lifecycle_state: "submitted"} = fetch_subject(subject.id)

    assert {:ok, executions} = Ash.read(ExecutionRecord)
    assert executions == []
  end

  test "advance/2 applies an execution-completed trigger and advances subject state" do
    installation = active_installation_fixture()

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:execution-completed",
        subject_kind: "expense_request",
        lifecycle_state: "processing",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-completed",
        causation_id: "cause-lifecycle-completed"
      })

    assert {:ok, result} =
             LifecycleEvaluator.advance(
               subject.id,
               trigger: {:execution_completed, "expense_capture"},
               execution_id: Ecto.UUID.generate(),
               causation_id: "cause-execution-completed"
             )

    assert result.action == :advanced_state
    assert result.from_state == "processing"
    assert result.to_state == "paid"
    assert result.trigger == %{"kind" => "execution_completed", "recipe_ref" => "expense_capture"}

    assert %{lifecycle_state: "paid"} = fetch_subject(subject.id)
  end

  test "advance/2 applies execution-failed transitions with specific failure kinds" do
    installation = active_installation_fixture(execution_failure_transition?: true)

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:execution-failed",
        subject_kind: "expense_request",
        lifecycle_state: "processing",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-failed",
        causation_id: "cause-lifecycle-failed"
      })

    assert {:ok, result} =
             LifecycleEvaluator.advance(
               subject.id,
               trigger: {:execution_failed, "expense_capture", :semantic_failure},
               execution_id: Ecto.UUID.generate(),
               causation_id: "cause-execution-failed"
             )

    assert result.action == :advanced_state
    assert result.to_state == "needs_correction"

    assert result.trigger == %{
             "kind" => "execution_failed",
             "recipe_ref" => "expense_capture",
             "failure_kind" => "semantic_failure"
           }

    assert %{lifecycle_state: "needs_correction"} = fetch_subject(subject.id)
  end

  test "advance/2 applies join-completed transitions through the explicit join trigger" do
    installation = active_installation_fixture(join_transition?: true)

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:join-completed",
        subject_kind: "expense_request",
        lifecycle_state: "awaiting_join",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-lifecycle-join",
        causation_id: "cause-lifecycle-join"
      })

    assert {:ok, result} =
             LifecycleEvaluator.advance(
               subject.id,
               trigger: {:join_completed, "triage_join"},
               execution_id: Ecto.UUID.generate(),
               causation_id: "cause-join-completed"
             )

    assert result.action == :advanced_state
    assert result.from_state == "awaiting_join"
    assert result.to_state == "paid"

    assert result.trigger == %{
             "kind" => "join_completed",
             "join_step_ref" => "triage_join"
           }

    assert %{lifecycle_state: "paid"} = fetch_subject(subject.id)
  end

  test "advance/2 creates a fresh linked execution for manual retry" do
    installation =
      active_installation_fixture(
        execution_failure_transition?: true,
        manual_retry_transition?: true
      )

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:manual-retry",
        subject_kind: "expense_request",
        lifecycle_state: "needs_correction",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-manual-retry",
        causation_id: "cause-manual-retry"
      })

    prior_execution =
      failed_execution_fixture(subject, installation, "trace-manual-retry", %{
        submission_dedupe_key: "manual-retry-prior",
        supersession_depth: 0
      })

    assert {:ok, result} =
             LifecycleEvaluator.advance(
               subject.id,
               supersedes_execution_id: prior_execution.id,
               supersession_reason: :manual_retry,
               causation_id: "cause-manual-retry:advance"
             )

    assert result.action == :queued_execution
    assert result.to_state == "processing"

    assert {:ok, execution} = Ash.get(ExecutionRecord, result.execution_id)
    assert execution.supersedes_execution_id == prior_execution.id
    assert execution.supersession_reason == :manual_retry
    assert execution.supersession_depth == 1
    assert execution.submission_dedupe_key != prior_execution.submission_dedupe_key

    assert %{lifecycle_state: "processing"} = fetch_subject(subject.id)
  end

  test "advance/2 blocks the subject when supersession depth exceeds the pack bound" do
    installation =
      active_installation_fixture(
        execution_failure_transition?: true,
        manual_retry_transition?: true,
        max_supersession_depth: 1
      )

    subject =
      subject_fixture(%{
        installation_id: installation.id,
        source_ref: "expense:request:cycle-bound",
        subject_kind: "expense_request",
        lifecycle_state: "needs_correction",
        payload: %{"amount_cents" => 12_500},
        trace_id: "trace-cycle-bound",
        causation_id: "cause-cycle-bound"
      })

    prior_execution =
      failed_execution_fixture(subject, installation, "trace-cycle-bound", %{
        submission_dedupe_key: "cycle-bound-prior",
        supersession_depth: 1
      })

    assert {:ok, result} =
             LifecycleEvaluator.advance(
               subject.id,
               supersedes_execution_id: prior_execution.id,
               supersession_reason: :manual_retry,
               causation_id: "cause-cycle-bound:advance"
             )

    assert result.action == :advanced_state
    assert result.to_state == "blocked_on_cycle"
    assert %{lifecycle_state: "blocked_on_cycle"} = fetch_subject(subject.id)
    assert execution_count(subject.id) == 1

    assert list_trace_fact_kinds(subject.id, "trace-cycle-bound") == [
             "subject_ingested",
             "lifecycle_advanced",
             "cycle_bound_reached"
           ]
  end

  defp active_installation_fixture(opts \\ []) do
    no_execution_request? = Keyword.get(opts, :no_execution_request?, false)
    execution_failure_transition? = Keyword.get(opts, :execution_failure_transition?, false)
    manual_retry_transition? = Keyword.get(opts, :manual_retry_transition?, false)
    join_transition? = Keyword.get(opts, :join_transition?, false)
    max_supersession_depth = Keyword.get(opts, :max_supersession_depth, 8)
    required_lifecycle_hints = Keyword.get(opts, :required_lifecycle_hints, [])
    produced_lifecycle_hints = Keyword.get(opts, :produced_lifecycle_hints, [])
    environment = Keyword.get(opts, :environment, "stage9")
    pack_version = Keyword.get(opts, :pack_version, "1.0.0")

    binding_config =
      Keyword.get(
        opts,
        :binding_config,
        %{
          "execution_bindings" => %{
            "expense_capture" => default_execution_binding(produced_lifecycle_hints)
          }
        }
      )

    compiled_pack =
      fixture_compiled_pack(
        no_execution_request?: no_execution_request?,
        execution_failure_transition?: execution_failure_transition?,
        manual_retry_transition?: manual_retry_transition?,
        join_transition?: join_transition?,
        max_supersession_depth: max_supersession_depth,
        required_lifecycle_hints: required_lifecycle_hints,
        pack_version: pack_version,
        runtime_class: Keyword.get(opts, :runtime_class, :session)
      )

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    registration_id = Ecto.UUID.generate()
    installation_id = Ecto.UUID.generate()

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

    %{
      id: installation_id,
      compiled_pack_revision: 1
    }
  end

  defp fixture_compiled_pack(opts) do
    no_execution_request? = Keyword.get(opts, :no_execution_request?, false)
    execution_failure_transition? = Keyword.get(opts, :execution_failure_transition?, false)
    manual_retry_transition? = Keyword.get(opts, :manual_retry_transition?, false)
    join_transition? = Keyword.get(opts, :join_transition?, false)
    max_supersession_depth = Keyword.get(opts, :max_supersession_depth, 8)
    required_lifecycle_hints = Keyword.get(opts, :required_lifecycle_hints, [])
    pack_version = Keyword.get(opts, :pack_version, "1.0.0")
    runtime_class = Keyword.get(opts, :runtime_class, :session)

    transitions =
      no_execution_request?
      |> base_transitions()
      |> maybe_add_failure_transitions(execution_failure_transition?, manual_retry_transition?)
      |> maybe_add_join_transition(join_transition?)

    manifest = %Manifest{
      pack_slug: :expense_approval,
      version: pack_version,
      max_supersession_depth: max_supersession_depth,
      profile_slots: profile_slots(),
      subject_kind_specs: [
        %SubjectKindSpec{name: :expense_request}
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :expense_request,
          initial_state: :submitted,
          terminal_states:
            terminal_states(
              execution_failure_transition?,
              manual_retry_transition?,
              join_transition?
            ),
          transitions: transitions
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: :expense_capture,
          runtime_class: runtime_class,
          placement_ref: :local_runner,
          required_lifecycle_hints: required_lifecycle_hints,
          workspace_policy: %{strategy: :per_subject, root_ref: :expense_workspaces},
          sandbox_policy_ref: :standard_expense_policy,
          prompt_refs: [:expense_capture_prompt],
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

  defp connector_capability_fixture([]), do: nil

  defp connector_capability_fixture(produced_lifecycle_hints) do
    %{
      "capability_id" => "expense.capture",
      "version" => "2026.04",
      "produces_lifecycle_hints" => Enum.map(produced_lifecycle_hints, &to_string/1)
    }
  end

  defp profile_slots do
    %{
      source_profile_ref: :fixture_source_v1,
      runtime_profile_ref: :fixture_runtime_v1,
      tool_scope_ref: :fixture_tools_v1,
      evidence_profile_ref: :fixture_evidence_v1,
      publication_profile_ref: :none,
      review_profile_ref: :operator_optional,
      memory_profile_ref: :none,
      projection_profile_ref: :runtime_readback_v1
    }
  end

  defp default_execution_binding(produced_lifecycle_hints) do
    %{
      "placement_ref" => "local_runner",
      "execution_params" => %{"timeout_ms" => 300_000},
      "authority_decision_ref" => "authority-decision://fixture/expense_capture",
      "connector_binding_ref" => "connector-binding://expense_system_api",
      "no_credentials_posture_ref" => "no-credentials://fixture/expense_capture",
      "connector_capability" => connector_capability_fixture(produced_lifecycle_hints),
      "connector_bindings" => %{
        "expense_system" => %{"connector_key" => "expense_system_api"}
      }
    }
  end

  defp execution_binding_config(binding) do
    %{"execution_bindings" => %{"expense_capture" => binding}}
  end

  defp base_transitions(true) do
    [
      %{from: :submitted, to: :submitted, trigger: :auto},
      %{from: :processing, to: :paid, trigger: {:execution_completed, :expense_capture}}
    ]
  end

  defp base_transitions(false) do
    [
      %{from: :submitted, to: :processing, trigger: {:execution_requested, :expense_capture}},
      %{from: :processing, to: :paid, trigger: {:execution_completed, :expense_capture}}
    ]
  end

  defp maybe_add_failure_transitions(transitions, false, _manual_retry_transition?),
    do: transitions

  defp maybe_add_failure_transitions(transitions, true, manual_retry_transition?) do
    transitions ++ failure_transitions(manual_retry_transition?)
  end

  defp failure_transitions(false) do
    [
      %{
        from: :processing,
        to: :needs_correction,
        trigger: {:execution_failed, :expense_capture, :semantic_failure}
      }
    ]
  end

  defp failure_transitions(true) do
    failure_transitions(false) ++
      [
        %{
          from: :needs_correction,
          to: :processing,
          trigger: {:execution_requested, :expense_capture}
        }
      ]
  end

  defp maybe_add_join_transition(transitions, false), do: transitions

  defp maybe_add_join_transition(transitions, true) do
    transitions ++
      [
        %{from: :awaiting_join, to: :paid, trigger: {:join_completed, :triage_join}}
      ]
  end

  defp terminal_states(execution_failure_transition?, manual_retry_transition?, _join_transition?) do
    if execution_failure_transition? and not manual_retry_transition? do
      [:paid, :needs_correction]
    else
      [:paid]
    end
  end

  defp subject_fixture(attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    subject_id = Ecto.UUID.generate()
    trace_id = Map.fetch!(attrs, :trace_id)
    causation_id = Map.fetch!(attrs, :causation_id)

    Repo.query!(
      """
      INSERT INTO subject_records (
        id,
        payload,
        installation_id,
        source_ref,
        subject_kind,
        lifecycle_state,
        schema_ref,
        schema_version,
        opened_at,
        row_version,
        inserted_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
      """,
      [
        dump_uuid!(subject_id),
        Map.fetch!(attrs, :payload),
        Map.fetch!(attrs, :installation_id),
        Map.fetch!(attrs, :source_ref),
        Map.fetch!(attrs, :subject_kind),
        Map.fetch!(attrs, :lifecycle_state),
        "mezzanine.subject.#{Map.fetch!(attrs, :subject_kind)}.payload.v1",
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
        trace_id,
        causation_id,
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
    %{
      rows: [[id, lifecycle_state, row_version]]
    } =
      Repo.query!(
        """
        SELECT id, lifecycle_state, row_version
        FROM subject_records
        WHERE id = $1::uuid
        """,
        [dump_uuid!(subject_id)]
      )

    %{
      id: id,
      lifecycle_state: lifecycle_state,
      row_version: row_version
    }
  end

  defp assert_no_dispatch_effects(subject_id) do
    assert %{lifecycle_state: "submitted"} = fetch_subject(subject_id)
    assert execution_count(subject_id) == 0
    assert workflow_start_outbox_count() == 0
    assert Repo.aggregate(Oban.Job, :count, :id) == 0
  end

  defp workflow_start_outbox_count do
    %{rows: [[count]]} = Repo.query!("SELECT count(*) FROM workflow_start_outbox")
    count
  end

  defp list_trace_fact_kinds(subject_id, trace_id) do
    %{
      rows: rows
    } =
      Repo.query!(
        """
        SELECT fact_kind
        FROM audit_facts
        WHERE subject_id = $1
          AND trace_id = $2
        ORDER BY occurred_at ASC, inserted_at ASC
        """,
        [subject_id, trace_id]
      )

    Enum.map(rows, fn [fact_kind] -> fact_kind end)
  end

  defp active_delayed_execution_fixture(subject, installation, trace_id, attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    execution_id = Ecto.UUID.generate()

    Repo.query!(
      """
      INSERT INTO execution_records (
        id,
        tenant_id,
        installation_id,
        subject_id,
        recipe_ref,
        compiled_pack_revision,
        binding_snapshot,
        dispatch_envelope,
        intent_snapshot,
        submission_dedupe_key,
        trace_id,
        causation_id,
        dispatch_state,
        dispatch_attempt_count,
        next_dispatch_at,
        submission_ref,
        lower_receipt,
        last_dispatch_error_kind,
        last_dispatch_error_payload,
        failure_kind,
        supersedes_execution_id,
        supersession_reason,
        supersession_depth,
        row_version,
        inserted_at,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'in_flight', 1, $13, $14,
        $15, $16, $17, NULL, NULL, NULL, 0, 1, $18, $18
      )
      """,
      [
        dump_uuid!(execution_id),
        "tenant-lifecycle-engine",
        installation.id,
        dump_uuid!(subject.id),
        "expense_capture",
        installation.compiled_pack_revision,
        %{"placement_ref" => "local_runner"},
        %{"recipe_ref" => "expense_capture"},
        %{"recipe_ref" => "expense_capture"},
        Map.fetch!(attrs, :submission_dedupe_key),
        trace_id,
        "cause:#{Map.fetch!(attrs, :submission_dedupe_key)}",
        Map.fetch!(attrs, :next_dispatch_at),
        %{},
        %{},
        "worker_crash",
        %{"error" => %{"kind" => "worker_crash"}},
        now
      ]
    )

    %{
      id: execution_id,
      submission_dedupe_key: Map.fetch!(attrs, :submission_dedupe_key)
    }
  end

  defp failed_execution_fixture(subject, installation, trace_id, attrs) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    execution_id = Ecto.UUID.generate()

    Repo.query!(
      """
      INSERT INTO execution_records (
        id,
        tenant_id,
        installation_id,
        subject_id,
        recipe_ref,
        compiled_pack_revision,
        binding_snapshot,
        dispatch_envelope,
        intent_snapshot,
        submission_dedupe_key,
        trace_id,
        causation_id,
        dispatch_state,
        dispatch_attempt_count,
        next_dispatch_at,
        submission_ref,
        lower_receipt,
        last_dispatch_error_kind,
        last_dispatch_error_payload,
        failure_kind,
        supersedes_execution_id,
        supersession_reason,
        supersession_depth,
        row_version,
        inserted_at,
        updated_at
      )
      VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 'failed', 1, NULL, $13, $14, $15,
        $16, 'semantic_failure', NULL, NULL, $17, 1, $18, $18
      )
      """,
      [
        dump_uuid!(execution_id),
        "tenant-lifecycle-engine",
        installation.id,
        dump_uuid!(subject.id),
        "expense_capture",
        installation.compiled_pack_revision,
        %{},
        %{"recipe_ref" => "expense_capture"},
        %{},
        Map.fetch!(attrs, :submission_dedupe_key),
        trace_id,
        "cause:#{Map.fetch!(attrs, :submission_dedupe_key)}",
        %{"id" => "submission:#{Map.fetch!(attrs, :submission_dedupe_key)}"},
        %{"state" => "failed", "run_id" => "run:#{Map.fetch!(attrs, :submission_dedupe_key)}"},
        "execution_failed",
        %{"error" => %{"kind" => "semantic_failure"}},
        Map.fetch!(attrs, :supersession_depth),
        now
      ]
    )

    %{
      id: execution_id,
      submission_dedupe_key: Map.fetch!(attrs, :submission_dedupe_key)
    }
  end

  defp execution_count(subject_id) do
    %{rows: [[count]]} =
      Repo.query!(
        """
        SELECT count(*)
        FROM execution_records
        WHERE subject_id = $1::uuid
        """,
        [dump_uuid!(subject_id)]
      )

    count
  end

  defp dump_uuid!(value), do: Ecto.UUID.dump!(value)
end
