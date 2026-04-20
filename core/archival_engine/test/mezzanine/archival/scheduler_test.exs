defmodule Mezzanine.Archival.SchedulerTest do
  use Mezzanine.Archival.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Mezzanine.Archival.{ArchivalManifest, Query, Repo, Scheduler}

  setup do
    root = Path.expand("../tmp/test_scheduler_store", __DIR__)
    File.rm_rf(root)

    Application.put_env(:mezzanine_archival_engine, :cold_store,
      module: Mezzanine.Archival.FileSystemColdStore,
      root: root
    )

    :ok
  end

  test "scheduler archives an eligible subject, removes hot rows, and leaves archived trace sources queryable" do
    installation_id = Ecto.UUID.generate()
    subject_id = Ecto.UUID.generate()
    execution_id = Ecto.UUID.generate()
    decision_id = Ecto.UUID.generate()
    evidence_id = Ecto.UUID.generate()
    audit_fact_id = Ecto.UUID.generate()
    terminal_at = ~U[2026-03-01 09:00:00Z]
    now = ~U[2026-04-16 12:00:00Z]

    insert_installation(installation_id, %{"hot_retention_days" => 30})
    insert_subject(subject_id, installation_id, terminal_at)
    insert_execution(execution_id, subject_id, installation_id, "trace-archive")
    insert_decision(decision_id, subject_id, execution_id, installation_id, "trace-archive")
    insert_evidence(evidence_id, subject_id, execution_id, installation_id, "trace-archive")

    insert_audit_fact(
      audit_fact_id,
      subject_id,
      execution_id,
      decision_id,
      evidence_id,
      installation_id,
      "trace-archive"
    )

    attach_telemetry(self())

    assert {:ok, result} = Scheduler.archive_subject(installation_id, subject_id, now: now)
    assert result.status == :archived
    assert result.trace_id == "trace-archive"
    assert result.removed.subjects == 1
    assert result.removed.executions == 1
    assert result.removed.decisions == 1
    assert result.removed.evidence == 1
    assert result.removed.audit_facts == 1

    assert hot_count("subject_records", subject_id) == 0
    assert hot_count("execution_records", execution_id) == 0
    assert hot_count("decision_records", decision_id) == 0
    assert hot_count("evidence_records", evidence_id) == 0
    assert hot_count("audit_facts", audit_fact_id) == 0

    assert {:ok, manifest} = Query.archived_subject_manifest(installation_id, subject_id)
    assert manifest.status == "archived"
    assert manifest.trace_ids == ["trace-archive"]
    assert File.exists?(manifest.storage_uri)

    assert {:ok, archived_trace} =
             Query.archived_trace_sources(installation_id, "trace-archive")

    assert archived_trace.manifest.manifest_ref == manifest.manifest_ref
    assert length(archived_trace.sources.audit_facts) == 1
    assert length(archived_trace.sources.executions) == 1
    assert length(archived_trace.sources.decisions) == 1
    assert length(archived_trace.sources.evidence) == 1

    assert archived_trace.sources.executions |> hd() |> Map.fetch!(:staleness_class) ==
             :authoritative_archived

    for {pivot, value} <- [
          trace_id: "trace-archive",
          subject_id: subject_id,
          execution_id: execution_id,
          decision_id: decision_id,
          run_id: "lower-run-#{execution_id}",
          attempt_id: "lower-attempt-#{execution_id}",
          artifact_id: "artifact-#{execution_id}",
          manifest_ref: manifest.manifest_ref
        ] do
      assert {:ok, pivot_trace} =
               Query.archived_trace_sources_by_pivot(installation_id, pivot, value)

      assert pivot_trace.trace_id == "trace-archive"
      assert pivot_trace.manifest.manifest_ref == manifest.manifest_ref
      assert length(pivot_trace.sources.executions) == 1
    end

    assert_receive {[:mezzanine, :archival, :run], %{count: 1}, run_meta}
    assert run_meta.trace_id == "trace-archive"
    assert run_meta.subject_id == subject_id

    assert_receive {[:mezzanine, :archival, :verified], %{count: 1}, verified_meta}
    assert verified_meta.trace_id == "trace-archive"

    assert_receive {[:mezzanine, :archival, :rows_removed], %{count: removed_count}, removed_meta}
    assert removed_count >= 5
    assert removed_meta.trace_id == "trace-archive"
  end

  test "scheduler preserves hot rows and marks the manifest failed when the cold store write fails" do
    installation_id = Ecto.UUID.generate()
    subject_id = Ecto.UUID.generate()
    execution_id = Ecto.UUID.generate()
    terminal_at = ~U[2026-03-01 09:00:00Z]
    now = ~U[2026-04-16 12:00:00Z]

    insert_installation(installation_id, %{"hot_retention_days" => 30})
    insert_subject(subject_id, installation_id, terminal_at)
    insert_execution(execution_id, subject_id, installation_id, "trace-failure")

    Application.put_env(:mezzanine_archival_engine, :cold_store,
      module: Mezzanine.Archival.FileSystemColdStore,
      root: "/dev/null/unwritable"
    )

    attach_telemetry(self())

    assert {:error, _reason} = Scheduler.archive_subject(installation_id, subject_id, now: now)

    assert hot_count("subject_records", subject_id) == 1
    assert hot_count("execution_records", execution_id) == 1

    assert {:ok, [manifest | _rest]} = ArchivalManifest.for_subject(installation_id, subject_id)
    assert manifest.status == "failed"

    assert_receive {[:mezzanine, :archival, :failed], %{count: 1}, failed_meta}
    assert failed_meta.trace_id == "trace-failure"
  end

  test "run_installation uses the default 30-day retention when installation metadata does not provide a valid override" do
    installation_id = Ecto.UUID.generate()
    due_subject_id = Ecto.UUID.generate()
    recent_subject_id = Ecto.UUID.generate()
    execution_id = Ecto.UUID.generate()
    due_terminal_at = ~U[2026-03-01 09:00:00Z]
    recent_terminal_at = ~U[2026-03-20 09:00:00Z]
    now = ~U[2026-04-16 12:00:00Z]

    insert_installation(installation_id, %{"hot_retention_days" => "bogus"})
    insert_subject(due_subject_id, installation_id, due_terminal_at, "ticket:due")
    insert_execution(execution_id, due_subject_id, installation_id, "trace-default-retention")
    insert_subject(recent_subject_id, installation_id, recent_terminal_at, "ticket:recent")

    assert %{
             installation_id: ^installation_id,
             subject_count: 1,
             archived: [{:ok, archived_result}]
           } = Scheduler.run_installation(installation_id, now: now)

    assert archived_result.status == :archived
    assert hot_count("subject_records", due_subject_id) == 0
    assert hot_count("subject_records", recent_subject_id) == 1

    assert {:ok, [manifest]} = ArchivalManifest.for_subject(installation_id, due_subject_id)
    assert manifest.status == "archived"
    assert manifest.retention_seconds == 30 * 86_400

    assert DateTime.compare(manifest.due_at, DateTime.add(due_terminal_at, 30 * 86_400, :second)) ==
             :eq

    assert {:ok, []} = ArchivalManifest.for_subject(installation_id, recent_subject_id)
  end

  defp attach_telemetry(test_pid) do
    handler_id = "archival-scheduler-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:mezzanine, :archival, :run],
        [:mezzanine, :archival, :verified],
        [:mezzanine, :archival, :failed],
        [:mezzanine, :archival, :rows_removed]
      ],
      &__MODULE__.handle_telemetry/4,
      test_pid
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {event, measurements, metadata})
  end

  defp insert_installation(installation_id, metadata) do
    pack_registration_id = Ecto.UUID.generate()
    insert_pack_registration(pack_registration_id)

    SQL.query!(
      Repo,
      """
      INSERT INTO installations (id, tenant_id, environment, pack_slug, pack_registration_id, status, compiled_pack_revision, binding_config, metadata, inserted_at, updated_at)
      VALUES ($1::uuid, 'tenant-1', 'default', 'pack', $2::uuid, 'active', 1, '{}'::jsonb, $3::jsonb, NOW(), NOW())
      """,
      [dump_uuid!(installation_id), dump_uuid!(pack_registration_id), Jason.encode!(metadata)]
    )
  end

  defp insert_pack_registration(pack_registration_id) do
    SQL.query!(
      Repo,
      """
      INSERT INTO pack_registrations (id, pack_slug, version, status, compiled_manifest, canonical_subject_kinds, serializer_version, migration_strategy, inserted_at, updated_at)
      VALUES ($1::uuid, 'pack', '1.0.0', 'active', '{}'::jsonb, ARRAY['ticket']::text[], 1, 'additive', NOW(), NOW())
      """,
      [dump_uuid!(pack_registration_id)]
    )
  end

  defp insert_subject(subject_id, installation_id, terminal_at, source_ref \\ "ticket:1") do
    SQL.query!(
      Repo,
      """
      INSERT INTO subject_records (id, installation_id, source_ref, subject_kind, lifecycle_state, status, status_updated_at, terminal_at, schema_ref, schema_version, payload, opened_at, inserted_at, updated_at, row_version)
      VALUES ($1::uuid, $2, $3, 'ticket', 'completed', 'cancelled', NOW(), $4, 'mezzanine.subject.ticket.payload.v1', 1, '{}'::jsonb, NOW(), NOW(), NOW(), 1)
      """,
      [dump_uuid!(subject_id), installation_id, source_ref, terminal_at]
    )
  end

  defp insert_execution(execution_id, subject_id, installation_id, trace_id) do
    SQL.query!(
      Repo,
      """
      INSERT INTO execution_records (id, tenant_id, installation_id, subject_id, recipe_ref, compiled_pack_revision, binding_snapshot, dispatch_envelope, intent_snapshot, dispatch_state, dispatch_attempt_count, submission_dedupe_key, lower_receipt, trace_id, row_version, inserted_at, updated_at)
      VALUES ($1::uuid, 'tenant-1', $2, $3::uuid, 'recipe', 1, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 'completed', 0, $4, $5::jsonb, $6, 1, NOW(), NOW())
      """,
      [
        dump_uuid!(execution_id),
        installation_id,
        dump_uuid!(subject_id),
        "submission-#{execution_id}",
        Jason.encode!(%{
          "run_id" => "lower-run-#{execution_id}",
          "attempt_id" => "lower-attempt-#{execution_id}",
          "artifact_ids" => ["artifact-#{execution_id}"]
        }),
        trace_id
      ]
    )
  end

  defp insert_decision(decision_id, subject_id, execution_id, installation_id, trace_id) do
    SQL.query!(
      Repo,
      """
      INSERT INTO decision_records (id, installation_id, subject_id, execution_id, decision_kind, lifecycle_state, decision_value, trace_id, row_version, inserted_at, updated_at)
      VALUES ($1::uuid, $2, $3::uuid, $4::uuid, 'review', 'resolved', 'accept', $5, 1, NOW(), NOW())
      """,
      [
        dump_uuid!(decision_id),
        installation_id,
        dump_uuid!(subject_id),
        dump_uuid!(execution_id),
        trace_id
      ]
    )
  end

  defp insert_evidence(evidence_id, subject_id, execution_id, installation_id, trace_id) do
    SQL.query!(
      Repo,
      """
      INSERT INTO evidence_records (id, installation_id, subject_id, execution_id, evidence_kind, status, metadata, trace_id, row_version, inserted_at, updated_at)
      VALUES ($1::uuid, $2, $3::uuid, $4::uuid, 'artifact', 'verified', '{}'::jsonb, $5, 1, NOW(), NOW())
      """,
      [
        dump_uuid!(evidence_id),
        installation_id,
        dump_uuid!(subject_id),
        dump_uuid!(execution_id),
        trace_id
      ]
    )
  end

  defp insert_audit_fact(
         audit_fact_id,
         subject_id,
         execution_id,
         decision_id,
         evidence_id,
         installation_id,
         trace_id
       ) do
    SQL.query!(
      Repo,
      """
      INSERT INTO audit_facts (id, installation_id, subject_id, execution_id, decision_id, evidence_id, trace_id, fact_kind, actor_ref, payload, occurred_at, inserted_at, updated_at)
      VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5::uuid, $6::uuid, $7, 'execution_completed', '{}'::jsonb, '{}'::jsonb, NOW(), NOW(), NOW())
      """,
      [
        dump_uuid!(audit_fact_id),
        installation_id,
        dump_uuid!(subject_id),
        dump_uuid!(execution_id),
        dump_uuid!(decision_id),
        dump_uuid!(evidence_id),
        trace_id
      ]
    )
  end

  defp hot_count(table, id) do
    sql = "SELECT COUNT(*) FROM #{table} WHERE id = $1::uuid"
    %{rows: [[count]]} = SQL.query!(Repo, sql, [dump_uuid!(id)])
    count
  end

  defp dump_uuid!(value) when is_binary(value), do: Ecto.UUID.dump!(value)
end
