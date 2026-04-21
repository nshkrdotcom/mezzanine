defmodule Mezzanine.OperatorActionsTest do
  use Mezzanine.Execution.DataCase, async: false

  require Ash.Query

  alias Mezzanine.Control.ControlSession
  alias Mezzanine.Execution.Repo, as: ExecutionRepo
  alias Mezzanine.Leasing
  alias Mezzanine.Leasing.AuthorizationScope
  alias Mezzanine.OperatorActions
  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Runs.{Run, RunSeries}
  alias Mezzanine.Work.{WorkClass, WorkObject}

  test "pause and resume create durable control state and interventions" do
    %{tenant_id: tenant_id, work_object: work_object} = fixture_stack("tenant-c")

    assert {:ok, %{control_session: paused, operator_actions: pause_actions}} =
             OperatorActions.pause_work(tenant_id, work_object.id, "ops_lead", %{
               "reason" => "hold"
             })

    assert paused.current_mode == :paused
    assert_declared_local_mutations!(pause_actions)

    assert {:ok, %{control_session: resumed, operator_actions: resume_actions}} =
             OperatorActions.resume_work(tenant_id, work_object.id, "ops_lead", %{
               "reason" => "continue"
             })

    assert resumed.current_mode == :normal
    assert_declared_local_mutations!(resume_actions)
  end

  test "pause_work invalidates subject leases even without an active execution" do
    %{tenant_id: tenant_id, work_object: work_object} = fixture_stack("tenant-c-leases")

    {:ok, read_lease} =
      Leasing.issue_read_lease(
        %{
          trace_id: stripped_uuid(),
          tenant_id: tenant_id,
          installation_id: Ecto.UUID.generate(),
          installation_revision: 1,
          activation_epoch: 1,
          lease_epoch: 1,
          subject_id: work_object.id,
          lineage_anchor: %{"submission_ref" => "sub-pause"},
          allowed_family: "unified_trace",
          allowed_operations: [:fetch_run]
        },
        repo: ExecutionRepo
      )

    {:ok, stream_lease} =
      Leasing.issue_stream_attach_lease(
        %{
          trace_id: stripped_uuid(),
          tenant_id: tenant_id,
          installation_id: Ecto.UUID.generate(),
          installation_revision: 1,
          activation_epoch: 1,
          lease_epoch: 1,
          subject_id: work_object.id,
          lineage_anchor: %{"submission_ref" => "sub-pause"},
          allowed_family: "runtime_stream"
        },
        repo: ExecutionRepo
      )

    assert {:ok, %{invalidated_lease_ids: invalidated_lease_ids, operator_actions: actions}} =
             OperatorActions.pause_work(tenant_id, work_object.id, "ops_lead", %{
               "reason" => "hold"
             })

    assert Enum.sort(invalidated_lease_ids) ==
             Enum.sort([read_lease.lease_id, stream_lease.lease_id])

    assert Enum.any?(actions, &match?(%{owner: :leasing}, &1))

    assert {:error, {:lease_invalidated, "subject_paused", _sequence_number}} =
             Leasing.authorize_read(
               read_authorization_scope(read_lease),
               read_lease.lease_id,
               read_lease.lease_token,
               :fetch_run,
               repo: ExecutionRepo
             )

    assert {:error, {:lease_invalidated, "subject_paused", _sequence_number}} =
             Leasing.authorize_stream_attach(
               stream_authorization_scope(stream_lease),
               stream_lease.lease_id,
               stream_lease.attach_token,
               repo: ExecutionRepo
             )
  end

  test "override_grant_profile persists the override set on the control session" do
    %{tenant_id: tenant_id, work_object: work_object} = fixture_stack("tenant-d")

    assert {:ok, %{control_session: control_session, operator_actions: actions}} =
             OperatorActions.override_grant_profile(tenant_id, work_object.id, "ops_lead", %{
               :"github.pr.write" => :approved
             })

    assert control_session.active_override_set == %{"github.pr.write" => "approved"}
    assert_declared_local_mutations!(actions)
  end

  test "request_replan supersedes the current plan and compiles a replacement" do
    %{tenant_id: tenant_id, work_object: work_object} = fixture_stack("tenant-e")

    assert {:ok, planned_work} =
             work_object
             |> Ash.Changeset.for_update(:compile_plan, %{})
             |> Ash.Changeset.set_tenant(tenant_id)
             |> Ash.update(
               actor: %{tenant_id: tenant_id},
               authorize?: false,
               domain: Mezzanine.Work
             )

    assert is_binary(planned_work.current_plan_id)

    assert {:ok,
            %{work_object: replanned_work, prior_plan: prior_plan, operator_actions: actions}} =
             OperatorActions.request_replan(tenant_id, planned_work.id, "ops_lead", %{
               "reason" => "policy change"
             })

    assert prior_plan.status == :superseded
    assert is_binary(replanned_work.current_plan_id)
    refute replanned_work.current_plan_id == prior_plan.id
    assert_declared_local_mutations!(actions)
  end

  test "cancel_work closes the current run and work object" do
    %{tenant_id: tenant_id, work_object: work_object} = fixture_stack("tenant-f")
    actor = %{tenant_id: tenant_id}

    {:ok, control_session} =
      ControlSession.open(
        %{program_id: work_object.program_id, work_object_id: work_object.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, run_series} =
      RunSeries.open_series(
        %{work_object_id: work_object.id, control_session_id: control_session.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, run} =
      Run.schedule(
        %{
          run_series_id: run_series.id,
          attempt: 1,
          runtime_profile: %{"runtime" => "session"},
          grant_profile: %{"connector" => "linear"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, run_series} =
      run_series
      |> Ash.Changeset.for_update(:attach_current_run, %{current_run_id: run.id})
      |> Ash.Changeset.set_tenant(tenant_id)
      |> Ash.update(actor: actor, authorize?: false, domain: Mezzanine.Runs)

    {:ok, _running_run} =
      run
      |> Ash.Changeset.for_update(:record_started, %{
        raw_runtime_ref: "runtime:cancel-me",
        started_at: DateTime.utc_now()
      })
      |> Ash.Changeset.set_tenant(tenant_id)
      |> Ash.update(actor: actor, authorize?: false, domain: Mezzanine.Runs)

    assert {:ok, %{work_object: cancelled_work, operator_actions: actions}} =
             OperatorActions.cancel_work(tenant_id, work_object.id, "ops_lead", %{})

    assert cancelled_work.status == :cancelled
    assert_declared_local_mutations!(actions)

    assert {:ok, cancelled_run} = fetch_run(tenant_id, run.id)
    assert cancelled_run.status == :cancelled
    assert {:ok, cancelled_series} = fetch_run_series(tenant_id, run_series.id)
    assert cancelled_series.status == :cancelled
  end

  defp fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "control-#{System.unique_integer([:positive])}",
          name: "Control Program",
          product_family: "operator_stack",
          configuration: %{},
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, bundle} =
      PolicyBundle.load_bundle(
        %{
          program_id: program.id,
          name: "default",
          version: "1.0.0",
          policy_kind: :workflow_md,
          source_ref: "WORKFLOW.md",
          body: workflow_body(),
          metadata: %{}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_class} =
      WorkClass.create_work_class(
        %{
          program_id: program.id,
          name: "coding_task_#{System.unique_integer([:positive])}",
          kind: "coding_task",
          intake_schema: %{"required" => ["title"]},
          policy_bundle_id: bundle.id,
          default_review_profile: %{"required" => true},
          default_run_profile: %{"runtime" => "session"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_object} =
      WorkObject.ingest(
        %{
          program_id: program.id,
          work_class_id: work_class.id,
          external_ref: "linear:ENG-#{System.unique_integer([:positive])}",
          title: "Control work",
          description: "Exercise the control service",
          priority: 50,
          source_kind: "linear",
          payload: %{"issue_id" => "ENG-1"},
          normalized_payload: %{"issue_id" => "ENG-1"}
        },
        actor: actor,
        tenant: tenant_id
      )

    %{
      tenant_id: tenant_id,
      actor: actor,
      program: program,
      bundle: bundle,
      work_class: work_class,
      work_object: work_object
    }
  end

  defp fetch_run(tenant_id, run_id) do
    Mezzanine.Runs.Run
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^run_id)
    |> Ash.read(actor: %{tenant_id: tenant_id}, domain: Mezzanine.Runs)
    |> case do
      {:ok, [run]} -> {:ok, run}
      {:ok, []} -> {:error, :not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp assert_declared_local_mutations!(actions) do
    assert actions != []

    assert Enum.all?(actions, fn action ->
             action.kind == :declared_local_mutation and
               is_binary(action.target_ref) and
               is_binary(action.release_manifest_ref)
           end)
  end

  defp fetch_run_series(tenant_id, run_series_id) do
    RunSeries
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(id == ^run_series_id)
    |> Ash.read(actor: %{tenant_id: tenant_id}, domain: Mezzanine.Runs)
    |> case do
      {:ok, [run_series]} -> {:ok, run_series}
      {:ok, []} -> {:error, :not_found}
      {:error, error} -> {:error, error}
    end
  end

  defp workflow_body do
    """
    ---
    tracker:
      kind: linear
      endpoint: https://api.linear.app/graphql
    run:
      profile: default_session
      runtime_class: session
      capability: linear.issue.execute
      target: linear-default
    approval:
      mode: manual
      reviewers:
        - ops_lead
      escalation_required: true
    retry:
      strategy: exponential
      max_attempts: 4
      initial_backoff_ms: 5000
      max_backoff_ms: 300000
    placement:
      profile_id: default-placement
      strategy: affinity
      target_selector:
        runtime_driver: jido_session
      runtime_preferences:
        locality: same_region
    workspace:
      root_mode: per_work
      sandbox_profile: strict
    review:
      required: true
      required_decisions: 1
      gates:
        - operator
    capability_grants:
      - capability_id: linear.issue.read
        mode: allow
      - capability_id: linear.issue.update
        mode: allow
    ---
    # Control prompt
    """
  end

  defp read_authorization_scope(read_lease) do
    AuthorizationScope.new!(
      tenant_id: read_lease.tenant_id,
      installation_id: read_lease.installation_id,
      installation_revision: read_lease.installation_revision,
      activation_epoch: read_lease.activation_epoch,
      lease_epoch: read_lease.lease_epoch,
      subject_id: read_lease.subject_id,
      execution_id: read_lease.execution_id,
      trace_id: read_lease.trace_id,
      actor_ref: %{id: "ops_lead"},
      authorized_at: DateTime.utc_now()
    )
  end

  defp stream_authorization_scope(stream_lease) do
    AuthorizationScope.new!(
      tenant_id: stream_lease.tenant_id,
      installation_id: stream_lease.installation_id,
      installation_revision: stream_lease.installation_revision,
      activation_epoch: stream_lease.activation_epoch,
      lease_epoch: stream_lease.lease_epoch,
      subject_id: stream_lease.subject_id,
      execution_id: stream_lease.execution_id,
      trace_id: stream_lease.trace_id,
      actor_ref: %{id: "ops_lead"},
      authorized_at: DateTime.utc_now()
    )
  end

  defp stripped_uuid do
    Ecto.UUID.generate()
    |> String.replace("-", "")
  end
end
