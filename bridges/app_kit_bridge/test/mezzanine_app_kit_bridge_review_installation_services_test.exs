defmodule Mezzanine.AppKitBridge.ReviewInstallationServicesTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Mezzanine.AppKitBridge
  alias Mezzanine.AppKitBridge.{InstallationService, ReviewActionService, ReviewQueryService}
  alias Mezzanine.ConfigRegistry.{PackRegistration, Repo}
  alias Mezzanine.Evidence.{EvidenceBundle, EvidenceItem}
  alias Mezzanine.OpsDomain.Repo, as: OpsRepo

  alias Mezzanine.Pack.{
    Compiler,
    ExecutionRecipeSpec,
    LifecycleSpec,
    Manifest,
    ProjectionSpec,
    SubjectKindSpec
  }

  alias Mezzanine.Programs.{PolicyBundle, Program}
  alias Mezzanine.Review.ReviewUnit
  alias Mezzanine.Runs.{Run, RunArtifact, RunSeries}
  alias Mezzanine.Work.{WorkClass, WorkObject}

  setup do
    ops_pid = Sandbox.start_owner!(OpsRepo, shared: false)
    config_pid = Sandbox.start_owner!(Repo, shared: true)
    allow_registry_process(config_pid)

    on_exit(fn ->
      Sandbox.stop_owner(ops_pid)
      Sandbox.stop_owner(config_pid)
    end)

    :ok
  end

  test "review services expose listing, detail, and decision flows without deprecated review surface" do
    %{tenant_id: tenant_id, program: program, review_unit: review_unit} =
      review_fixture_stack("tenant-bridge-review")

    assert {:ok, listings} = ReviewQueryService.list_pending_reviews(tenant_id, program.id)
    assert Enum.any?(listings, &(&1.decision_ref.id == review_unit.id))

    assert {:ok, detail} = ReviewQueryService.get_review_detail(tenant_id, review_unit.id)
    assert detail.decision_ref.id == review_unit.id
    assert detail.subject_ref.id == review_unit.work_object_id
    assert detail.status == "pending"
    assert detail.payload.review_unit.id == review_unit.id
    assert length(detail.payload.evidence_items) == 1
    assert length(detail.payload.run_artifacts) == 1

    assert {:ok, action_result} =
             ReviewActionService.record_decision(tenant_id, review_unit.id, %{
               program_id: program.id,
               decision: :accept,
               actor_ref: "ops_lead",
               reason: "looks good"
             })

    assert action_result.status == :completed
    assert action_result.action_ref.action_kind == "review_accept"
    assert action_result.metadata.review_unit.status == :accepted

    assert {:ok, bridge_detail} = AppKitBridge.get_review_detail(tenant_id, review_unit.id)
    assert bridge_detail.decision_ref.id == review_unit.id
  end

  test "installation service creates, lists, updates, suspends, and reactivates active pack installations" do
    activate_fixture_registration!("1.0.0")

    attrs = %{
      tenant_id: "tenant-install",
      environment: "prod",
      template_key: "expense-default",
      pack_slug: "expense_approval",
      pack_version: "1.0.0",
      default_bindings: %{
        "execution_bindings" => %{
          "expense_capture" => %{
            "placement_ref" => "local_docker"
          }
        }
      },
      metadata: %{"managed_by" => "bridge-test"}
    }

    assert {:ok, install_result} = InstallationService.create_installation(attrs)
    assert install_result.status == :created
    assert install_result.installation_ref.status == :active
    assert install_result.installation_ref.pack_slug == "expense_approval"
    assert install_result.installation_ref.pack_version == "1.0.0"

    installation_id = install_result.installation_ref.id

    assert {:ok, reused_result} = InstallationService.create_installation(attrs)
    assert reused_result.status == :reused
    assert reused_result.installation_ref.id == installation_id

    assert {:ok, installations} = InstallationService.list_installations("tenant-install")
    assert Enum.any?(installations, &(&1.installation_ref.id == installation_id))

    assert {:ok, detail} = InstallationService.get_installation(installation_id)
    assert detail.installation_ref.id == installation_id
    assert detail.environment == "prod"

    assert detail.bindings["execution_bindings"]["expense_capture"]["placement_ref"] ==
             "local_docker"

    assert {:ok, update_result} =
             InstallationService.update_bindings(installation_id, %{
               "execution_bindings" => %{
                 "expense_capture" => %{
                   "placement_ref" => "remote_runner"
                 }
               }
             })

    assert update_result.status == :completed
    assert update_result.metadata.installation.installation_ref.compiled_pack_revision == 2

    assert update_result.metadata.installation.bindings["execution_bindings"]["expense_capture"][
             "placement_ref"
           ] == "remote_runner"

    assert {:ok, suspend_result} = InstallationService.suspend_installation(installation_id)
    assert suspend_result.metadata.installation.installation_ref.status == :suspended

    assert {:ok, reactivate_result} = InstallationService.reactivate_installation(installation_id)
    assert reactivate_result.metadata.installation.installation_ref.status == :active

    assert {:ok, bridge_installation} = AppKitBridge.get_installation(installation_id)
    assert bridge_installation.installation_ref.id == installation_id
  end

  test "installation service refuses to deploy or install unactivated pack registrations" do
    MezzanineConfigRegistry.register_pack!(compiled_pack_fixture("2.0.0"))

    assert {:error, :pack_registration_not_active} =
             InstallationService.create_installation(%{
               tenant_id: "tenant-install-inactive",
               pack_slug: "expense_approval",
               pack_version: "2.0.0"
             })
  end

  defp review_fixture_stack(tenant_id) do
    actor = %{tenant_id: tenant_id}

    {:ok, program} =
      Program.create_program(
        %{
          slug: "bridge-review-#{System.unique_integer([:positive])}",
          name: "Bridge Review Program",
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
          title: "Review work",
          description: "Exercise review bridge services",
          priority: 50,
          source_kind: "linear",
          payload: %{"issue_id" => "ENG-1"},
          normalized_payload: %{"issue_id" => "ENG-1"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, work_object} =
      WorkObject.compile_plan(work_object, %{}, actor: actor, tenant: tenant_id)

    {:ok, run_series} =
      RunSeries.open_series(%{work_object_id: work_object.id}, actor: actor, tenant: tenant_id)

    {:ok, run} =
      Run.schedule(
        %{
          run_series_id: run_series.id,
          attempt: 1,
          runtime_profile: %{"runtime" => "session"},
          grant_profile: %{"linear.issue.update" => "allow"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _run_series} =
      RunSeries.attach_current_run(run_series, %{current_run_id: run.id},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, evidence_bundle} =
      EvidenceBundle.assemble(
        %{
          program_id: program.id,
          work_object_id: work_object.id,
          run_id: run.id,
          summary: "bundle ready",
          evidence_manifest: %{},
          completeness_status: %{},
          assembled_at: DateTime.utc_now()
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _evidence_item} =
      EvidenceItem.record_item(
        %{evidence_bundle_id: evidence_bundle.id, kind: :diff, ref: "diff://1", metadata: %{}},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _run_artifact} =
      RunArtifact.record_artifact(
        %{run_id: run.id, kind: :pr, ref: "https://github.com/example/pr/1", metadata: %{}},
        actor: actor,
        tenant: tenant_id
      )

    {:ok, review_unit} =
      ReviewUnit.create_review_unit(
        %{
          work_object_id: work_object.id,
          run_id: run.id,
          review_kind: :operator_review,
          required_by: DateTime.utc_now(),
          decision_profile: %{"required_decisions" => 1},
          evidence_bundle_id: evidence_bundle.id,
          reviewer_actor: %{"kind" => "human", "ref" => "ops_lead"}
        },
        actor: actor,
        tenant: tenant_id
      )

    {:ok, _audit} =
      Mezzanine.Audit.record_event(tenant_id, %{
        program_id: program.id,
        work_object_id: work_object.id,
        run_id: run.id,
        review_unit_id: review_unit.id,
        event_kind: :review_created,
        actor_kind: :system,
        actor_ref: "planner",
        payload: %{"gate" => "operator"},
        occurred_at: ~U[2026-04-14 21:10:01Z]
      })

    %{
      tenant_id: tenant_id,
      program: program,
      review_unit: review_unit
    }
  end

  defp activate_fixture_registration!(version) do
    compiled_pack_fixture(version)
    |> MezzanineConfigRegistry.register_pack!()
    |> PackRegistration.activate()
    |> case do
      {:ok, registration} -> registration
      {:error, error} -> raise "failed to activate fixture registration: #{inspect(error)}"
    end
  end

  defp compiled_pack_fixture(version) do
    manifest = %Manifest{
      pack_slug: :expense_approval,
      version: version,
      subject_kind_specs: [
        %SubjectKindSpec{name: :expense_request}
      ],
      lifecycle_specs: [
        %LifecycleSpec{
          subject_kind: :expense_request,
          initial_state: :submitted,
          terminal_states: [:paid],
          transitions: [
            %{
              from: :submitted,
              to: :processing,
              trigger: {:execution_requested, :expense_capture}
            },
            %{from: :processing, to: :paid, trigger: {:execution_completed, :expense_capture}}
          ]
        }
      ],
      execution_recipe_specs: [
        %ExecutionRecipeSpec{
          recipe_ref: :expense_capture,
          runtime_class: :session,
          placement_ref: :local_runner
        }
      ],
      projection_specs: [
        %ProjectionSpec{name: :active_expenses, subject_kinds: [:expense_request]}
      ]
    }

    case Compiler.compile(manifest) do
      {:ok, compiled_pack} -> compiled_pack
      {:error, errors} -> raise "failed to compile pack fixture: #{inspect(errors)}"
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
    # Operator Prompt
    """
  end

  defp allow_registry_process(config_pid) do
    case Process.whereis(Mezzanine.Pack.Registry) do
      pid when is_pid(pid) -> Sandbox.allow(Repo, config_pid, pid)
      _other -> :ok
    end
  end
end
