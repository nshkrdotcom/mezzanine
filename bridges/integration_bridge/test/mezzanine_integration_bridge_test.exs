defmodule Mezzanine.IntegrationBridgeTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Jido.Integration.V2.GovernedLowerDenial
  alias Jido.Integration.V2.GovernedLowerEnvelope
  alias Jido.Integration.V2.GovernedLowerReceipt
  alias Jido.Integration.V2.TenantScope
  alias Mezzanine.Audit.{ExecutionLineage, ExecutionLineageStore, Repo}
  alias Mezzanine.IntegrationBridge
  alias Mezzanine.IntegrationBridge.AuthorizedInvocation
  alias Mezzanine.Intent.{EffectIntent, ReadIntent, RunIntent}

  defmodule LowerFactsStub do
    @operations [
      :fetch_submission_receipt,
      :fetch_run,
      :attempts,
      :fetch_attempt,
      :events,
      :fetch_artifact,
      :run_artifacts,
      :resolve_trace
    ]

    def operations, do: @operations
    def operation_supported?(operation), do: operation in @operations

    def fetch_submission_receipt(%TenantScope{} = scope, submission_key) do
      dispatch(:fetch_submission_receipt, [scope, submission_key], %{
        submission_key: submission_key
      })
    end

    def fetch_run(%TenantScope{} = scope, run_id) do
      dispatch(:fetch_run, [scope, run_id], %{run_id: run_id, status: :completed})
    end

    def attempts(%TenantScope{} = scope, run_id) do
      dispatch(:attempts, [scope, run_id], [
        %{attempt_id: "attempt-1", run_id: run_id, status: :completed}
      ])
    end

    def fetch_attempt(%TenantScope{} = scope, attempt_id) do
      dispatch(:fetch_attempt, [scope, attempt_id], %{
        attempt_id: attempt_id,
        run_id: "run-1",
        status: :completed
      })
    end

    def events(%TenantScope{} = scope, run_id) do
      dispatch(:events, [scope, run_id], [%{run_id: run_id, type: "attempt.completed"}])
    end

    def fetch_artifact(%TenantScope{} = scope, artifact_id) do
      dispatch(:fetch_artifact, [scope, artifact_id], %{artifact_id: artifact_id, run_id: "run-1"})
    end

    def run_artifacts(%TenantScope{} = scope, run_id) do
      dispatch(:run_artifacts, [scope, run_id], [%{artifact_id: "artifact-1", run_id: run_id}])
    end

    def resolve_trace(%TenantScope{} = scope, trace_id) do
      dispatch(:resolve_trace, [scope, trace_id], %{
        trace_id: trace_id,
        run: %{run_id: "run-1"}
      })
    end

    defp dispatch(operation, args, fallback) do
      send(Process.get(:integration_bridge_test_pid), {operation, args})

      case Process.get(:integration_bridge_test_responses, %{}) do
        %{^operation => handler} when is_function(handler, 1) -> handler.(args)
        _other -> default_reply(fallback)
      end
    end

    defp default_reply(value) when is_list(value), do: value
    defp default_reply(value), do: {:ok, value}
  end

  setup do
    owner = Sandbox.start_owner!(Repo, shared: true)

    Process.put(:integration_bridge_test_pid, self())
    Process.put(:integration_bridge_test_responses, %{})

    on_exit(fn ->
      Process.delete(:integration_bridge_test_pid)
      Process.delete(:integration_bridge_test_responses)
      Sandbox.stop_owner(owner)
    end)

    :ok
  end

  test "invoke_run_intent dispatches only an authorized invocation envelope" do
    invocation = authorized_invocation()

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issues.retrieve"}} =
             IntegrationBridge.invoke_run_intent(
               invocation,
               invoke_fun: invoke_fun,
               invoke_opts: [connection_id: "conn-1"]
             )

    assert_received {:invoke, "linear.issues.retrieve", input, opts}
    assert Keyword.fetch!(opts, :connection_id) == "conn-1"
    assert input.invocation_request == invocation.invocation_request
    assert input.idempotency_key == "idem-1"
    assert input.submission_dedupe_key == "dedupe-1"
    assert input.authority.permission_decision_ref == "mock-decision-123"
    assert input.authority.policy_version == "mock-v1"
  end

  test "Linear source candidate fetch uses governed direct connector dispatch and SourceEngine normalization" do
    invocation = authorized_invocation_allowing(["linear.issues.list"])

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})

      {:ok,
       %{
         output: %{
           issues: [linear_issue()],
           page_info: %{has_next_page: false}
         },
         artifact_refs: ["artifact://linear/issues-list"]
       }}
    end

    assert {:ok, result} =
             IntegrationBridge.fetch_linear_candidates(
               invocation,
               source_binding(),
               invoke_fun: invoke_fun,
               viewer: %{id: "usr-linear-viewer"}
             )

    assert_received {:invoke, "linear.issues.list", input, opts}
    assert input.filter.state_names == ["Todo", "Backlog"]
    assert input.filter.assignee_id == "usr-linear-viewer"
    assert input.governed_lower_envelope["lower_runtime_kind"] == "direct_connector"
    assert Keyword.fetch!(opts, :governed_lower_envelope).capability_id == "linear.issues.list"

    assert result.source_intake.operation == "linear.issues.list"
    assert [%{source_ref: "linear://inst-1/issue/ENG-321"}] = result.source_intake.subject_attrs
  end

  test "Linear source candidate fetch resolves viewer before assignee-me intake" do
    invocation = authorized_invocation_allowing(["linear.users.get_self", "linear.issues.list"])

    invoke_fun = fn
      "linear.users.get_self", input, _opts ->
        send(self(), {:invoke, "linear.users.get_self", input})
        {:ok, %{output: %{user: %{id: "usr-linear-viewer", name: "Taylor Automation"}}}}

      "linear.issues.list", input, _opts ->
        send(self(), {:invoke, "linear.issues.list", input})
        {:ok, %{output: %{issues: [linear_issue()]}}}
    end

    assert {:ok, result} =
             IntegrationBridge.fetch_linear_candidates(
               invocation,
               source_binding(),
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.users.get_self", %{}}

    assert_received {:invoke, "linear.issues.list",
                     %{filter: %{assignee_id: "usr-linear-viewer"}}}

    assert result.viewer_resolution.output.user.id == "usr-linear-viewer"
    assert [%{lifecycle_state: "submitted"}] = result.source_intake.subject_attrs
  end

  test "Linear issue refresh normalizes provider output into source subject attrs" do
    invocation = authorized_invocation_allowing(["linear.issues.retrieve"])

    invoke_fun = fn capability, input, _opts ->
      send(self(), {:invoke, capability, input})
      {:ok, %{output: %{issue: linear_issue()}}}
    end

    assert {:ok, result} =
             IntegrationBridge.refresh_linear_issue(
               invocation,
               "lin-issue-321",
               source_binding(),
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.issues.retrieve", %{issue_id: "lin-issue-321"}}
    assert result.source_refresh.operation == "linear.issues.retrieve"
    assert result.source_refresh.subject_attrs.provider_external_ref == "lin-issue-321"
  end

  test "Linear source publication emits a public-safe governed publication receipt" do
    invocation = authorized_invocation_allowing(["linear.comments.update"])

    invoke_fun = fn capability, input, _opts ->
      send(self(), {:invoke, capability, input})
      {:ok, %{output: %{success: true, comment: %{id: "comment-1"}}}}
    end

    assert {:ok, result} =
             IntegrationBridge.publish_linear_source(
               invocation,
               %{
                 source_publish_ref: "linear_workpad_review",
                 source_binding_id: "linear-primary",
                 source_ref: "linear://inst-1/issue/ENG-321",
                 comment_id: "comment-1",
                 body: "Ready for review",
                 redaction_manifest_ref: "redaction://linear/workpad"
               },
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.comments.update", %{comment_id: "comment-1"}}

    receipt = result.source_publication_receipt
    assert receipt.status == "published"
    assert receipt.capability_id == "linear.comments.update"
    assert receipt.lower_runtime_kind == "direct_connector"
    assert receipt.authority_ref == "authority-decision://mock-decision-123"
    assert receipt.connector_manifest_ref == "manifest://jido/connectors/linear@local"
    assert receipt.redaction_manifest_ref == "redaction://linear/workpad"
    assert receipt.workpad_refs == ["linear-comment://comment-1"]
  end

  test "Linear source publication can create a workpad comment after update miss" do
    invocation =
      authorized_invocation_allowing(["linear.comments.update", "linear.comments.create"])

    invoke_fun = fn
      "linear.comments.update", input, _opts ->
        send(self(), {:invoke, "linear.comments.update", input})
        {:error, %{reason: %{code: "linear.not_found"}}}

      "linear.comments.create", input, _opts ->
        send(self(), {:invoke, "linear.comments.create", input})
        {:ok, %{output: %{success: true, comment: %{id: "comment-created"}}}}
    end

    assert {:ok, result} =
             IntegrationBridge.publish_linear_source(
               invocation,
               %{
                 source_publish_ref: "linear_workpad_review",
                 source_binding_id: "linear-primary",
                 source_ref: "linear://inst-1/issue/ENG-321",
                 issue_id: "lin-issue-321",
                 comment_id: "stale-comment",
                 body: "Ready for review",
                 allow_create_fallback?: true
               },
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "linear.comments.update", %{comment_id: "stale-comment"}}
    assert_received {:invoke, "linear.comments.create", %{issue_id: "lin-issue-321"}}

    assert result.source_publication_receipt.capability_id == "linear.comments.create"
    assert result.source_publication_receipt.fallback_from == "linear.comments.update"
    assert result.source_publication_receipt.workpad_refs == ["linear-comment://comment-created"]
  end

  test "GitHub PR creation uses governed direct connector dispatch" do
    invocation = authorized_invocation_allowing(["github.pr.create"])

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})
      {:ok, %{output: github_pr(), artifact_refs: ["artifact://github/pr-create"]}}
    end

    assert {:ok, result} =
             IntegrationBridge.create_github_pr(
               invocation,
               %{
                 repo: "nshkrdotcom/extravaganza",
                 title: "Governed GitHub PR",
                 body: "Created through the direct connector lane",
                 head: "phase-7",
                 base: "main",
                 draft: true
               },
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "github.pr.create", input, opts}
    assert input.repo == "nshkrdotcom/extravaganza"
    assert input.governed_lower_envelope["lower_runtime_kind"] == "direct_connector"

    assert input.governed_lower_envelope["connector_manifest_ref"] ==
             "manifest://jido/connectors/github@local"

    envelope = Keyword.fetch!(opts, :governed_lower_envelope)
    assert envelope.capability_id == "github.pr.create"
    assert envelope.connector_ref == "jido/connectors/github"

    assert result.github_operation_receipt.capability_id == "github.pr.create"
    assert result.github_operation_receipt.capability_negotiation_ref =~ "cap-neg://"
    assert result.github_operation_receipt.provider_response_ref == "artifact://github/pr-create"
  end

  test "GitHub PR feedback sweep reads reviews, comments, statuses, and checks" do
    capabilities = [
      "github.pr.reviews.list",
      "github.pr.review_comments.list",
      "github.commit.statuses.get_combined",
      "github.check_runs.list_for_ref"
    ]

    invocation = authorized_invocation_allowing(capabilities)

    invoke_fun = fn
      "github.pr.reviews.list", input, _opts ->
        send(self(), {:invoke, "github.pr.reviews.list", input})
        {:ok, %{output: github_reviews(), artifact_refs: ["artifact://github/reviews"]}}

      "github.pr.review_comments.list", input, _opts ->
        send(self(), {:invoke, "github.pr.review_comments.list", input})
        {:ok, %{output: github_review_comments(), artifact_refs: ["artifact://github/comments"]}}

      "github.commit.statuses.get_combined", input, _opts ->
        send(self(), {:invoke, "github.commit.statuses.get_combined", input})
        {:ok, %{output: github_status(), artifact_refs: ["artifact://github/status"]}}

      "github.check_runs.list_for_ref", input, _opts ->
        send(self(), {:invoke, "github.check_runs.list_for_ref", input})
        {:ok, %{output: github_checks(), artifact_refs: ["artifact://github/checks"]}}
    end

    assert {:ok, %{github_feedback_sweep: sweep}} =
             IntegrationBridge.sweep_github_pr_feedback(
               invocation,
               %{repo: "nshkrdotcom/extravaganza", pull_number: 17, ref: "head-sha"},
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "github.pr.reviews.list",
                     %{repo: "nshkrdotcom/extravaganza", pull_number: 17}}

    assert_received {:invoke, "github.commit.statuses.get_combined",
                     %{repo: "nshkrdotcom/extravaganza", ref: "head-sha"}}

    assert sweep.review_count == 2
    assert sweep.review_comment_count == 1
    assert sweep.combined_state == "success"
    assert sweep.check_run_count == 1
    assert Enum.map(sweep.operation_receipts, & &1.capability_id) == capabilities
  end

  test "GitHub branch cleanup is a governed delete-ref operation" do
    invocation = authorized_invocation_allowing(["github.git.ref.delete"])

    invoke_fun = fn capability, input, _opts ->
      send(self(), {:invoke, capability, input})
      {:ok, %{output: %{repo: input.repo, ref: input.ref, deleted?: true}}}
    end

    assert {:ok, result} =
             IntegrationBridge.cleanup_github_branch(
               invocation,
               %{repo: "nshkrdotcom/extravaganza", ref: "heads/phase-7"},
               invoke_fun: invoke_fun
             )

    assert_received {:invoke, "github.git.ref.delete",
                     %{repo: "nshkrdotcom/extravaganza", ref: "heads/phase-7"}}

    assert result.github_operation_receipt.capability_id == "github.git.ref.delete"
    assert result.github_operation_receipt.lower_runtime_kind == "direct_connector"
  end

  test "invoke_run_intent builds a governed lower envelope and receipt around dispatch" do
    invocation = authorized_invocation()

    invoke_fun = fn capability, input, opts ->
      send(self(), {:invoke, capability, input, opts})
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok,
            %{
              governed_lower_envelope: %GovernedLowerEnvelope{} = envelope,
              governed_lower_receipt: %GovernedLowerReceipt{} = receipt
            }} =
             IntegrationBridge.invoke_run_intent(
               invocation,
               invoke_fun: invoke_fun,
               capability_id: "linear.issues.retrieve",
               lower_runtime_kind: :deterministic_fixture,
               policy_bundle_ref: "policy-bundle://extravaganza/default",
               policy_bundle_hash:
                 "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
               cedar_schema_ref: "cedar-schema://extravaganza/source",
               cedar_schema_hash:
                 "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
               script_ref: "script://linear/retrieve",
               script_hash:
                 "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
               package_refs: ["package://extravaganza/coding-ops"],
               sandbox_profile_ref: "sandbox://local/strict",
               attestation_requirement_ref: "attestation://local/dev"
             )

    assert envelope.capability_id == "linear.issues.retrieve"
    assert envelope.lower_runtime_kind == :deterministic_fixture
    assert envelope.authority_ref == "authority-decision://mock-decision-123"
    assert envelope.authority_decision_hash == String.duplicate("a", 64)
    assert envelope.allowed_operations == ["linear.issues.retrieve", "linear.issues.update"]
    assert envelope.resource_scope_refs == ["workspace://work_object/subject-1"]
    assert receipt.status == :succeeded
    assert receipt.policy_bundle_ref == "policy-bundle://extravaganza/default"
    assert receipt.cedar_schema_ref == "cedar-schema://extravaganza/source"
    assert receipt.script_ref == "script://linear/retrieve"
    assert receipt.package_refs == ["package://extravaganza/coding-ops"]
    assert receipt.resource_scope_refs == ["workspace://work_object/subject-1"]
    assert receipt.sandbox_profile_ref == "sandbox://local/strict"
    assert receipt.attestation_requirement_ref == "attestation://local/dev"
    assert GovernedLowerReceipt.matches_envelope?(receipt, envelope)

    assert_received {:invoke, "linear.issues.retrieve", input, opts}
    assert input.governed_lower_envelope["lower_request_ref"] == envelope.lower_request_ref
    assert Keyword.fetch!(opts, :governed_lower_envelope) == envelope
  end

  test "dispatch_effect dispatches only an authorized invocation envelope" do
    invocation = authorized_invocation()

    invoke_fun = fn capability, input, _opts ->
      {:ok, %{capability: capability, input: input}}
    end

    assert {:ok, %{capability: "linear.issues.update"}} =
             IntegrationBridge.dispatch_effect(invocation,
               invoke_fun: invoke_fun,
               capability_id: "linear.issues.update"
             )
  end

  test "direct lower dispatch returns governed denials before side effects" do
    never = fn _capability, _input, _opts -> flunk("provider invoke must not run") end

    assert {:error,
            %GovernedLowerDenial{
              denial_class: :lower_runtime_unavailable,
              lower_runtime_kind: :tre_rhai
            }} =
             IntegrationBridge.invoke_run_intent(authorized_invocation(),
               invoke_fun: never,
               lower_runtime_kind: :tre_rhai
             )

    assert {:error, %GovernedLowerDenial{denial_class: :resource_scope_unresolvable}} =
             IntegrationBridge.invoke_run_intent(authorized_invocation(),
               invoke_fun: never,
               resource_scope_refs: ["unresolved://workspace/main"]
             )

    assert {:error, %GovernedLowerDenial{denial_class: :sandbox_downgrade}} =
             IntegrationBridge.invoke_run_intent(authorized_invocation_with_governance_posture(),
               invoke_fun: never,
               sandbox_level: :none
             )

    assert {:error, %GovernedLowerDenial{denial_class: :attestation_unsatisfied}} =
             IntegrationBridge.invoke_run_intent(authorized_invocation_with_governance_posture(),
               invoke_fun: never,
               acceptable_attestation: ["attestation://unexpected"]
             )
  end

  test "direct dispatch rejects old RunIntent and generic map inputs before Jido invocation" do
    intent =
      RunIntent.new!(%{
        intent_id: "intent-run-1",
        program_id: "program-1",
        work_id: "work-1",
        capability: "linear.issues.retrieve",
        input: %{"issue_id" => "ENG-42"}
      })

    invoke_fun = fn _capability, _input, _opts -> flunk("provider invoke must not run") end

    assert_raise FunctionClauseError, fn ->
      IntegrationBridge.invoke_run_intent(intent, invoke_fun: invoke_fun)
    end

    assert_raise FunctionClauseError, fn ->
      IntegrationBridge.invoke_run_intent(%{}, invoke_fun: invoke_fun)
    end
  end

  test "effect dispatch rejects old EffectIntent and unauthorized capability inputs" do
    intent =
      EffectIntent.new!(%{
        intent_id: "effect-1",
        effect_type: :connector_effect,
        subject: "issue",
        payload: %{
          capability_id: "linear.issues.update",
          input: %{"id" => "ENG-42", "state" => "done"}
        }
      })

    invoke_fun = fn _capability, _input, _opts -> flunk("provider invoke must not run") end

    assert_raise FunctionClauseError, fn ->
      IntegrationBridge.dispatch_effect(intent, invoke_fun: invoke_fun)
    end

    assert_error_contains("not present in Citadel authority", fn ->
      IntegrationBridge.dispatch_effect(authorized_invocation(),
        invoke_fun: invoke_fun,
        capability_id: "github.pr.merge"
      )
    end)
  end

  test "authorized invocation requires mock-valid authority and governance packets" do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :authority_packet], %{})

    assert_error_contains("missing required field :contract_version", fn ->
      AuthorizedInvocation.new!(attrs)
    end)
  end

  test "authorized invocation rejects tenant and trace mismatches before lower dispatch" do
    assert_error_contains("tenant_id mismatch", fn ->
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :tenant_id], "tenant-other")
      |> AuthorizedInvocation.new!()
    end)

    assert_error_contains("trace_id mismatch", fn ->
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :trace_id], "trace-other")
      |> AuthorizedInvocation.new!()
    end)
  end

  test "authorized invocation rejects stale installation revision when caller supplies one" do
    assert %AuthorizedInvocation{} =
             AuthorizedInvocation.new!(
               Map.put(authorized_invocation_attrs(), :expected_installation_revision, 3)
             )

    assert_error_contains("stale installation_revision", fn ->
      authorized_invocation_attrs()
      |> Map.put(:expected_installation_revision, 2)
      |> AuthorizedInvocation.new!()
    end)
  end

  test "authorized invocation binds M2 lower submission to the Citadel for_action_ref" do
    attrs =
      authorized_invocation_attrs()
      |> Map.put(:action_ref, "action://agent-loop/turn-1")
      |> put_in([:invocation_request, :authority_packet, :extensions, "citadel"], %{
        "for_action_ref" => "action://agent-loop/turn-1"
      })
      |> put_in([:invocation_request, :execution_governance, :extensions, "citadel"], %{
        "for_action_ref" => "action://agent-loop/turn-1"
      })

    invocation = AuthorizedInvocation.new!(attrs)
    input = AuthorizedInvocation.invoke_input(invocation, "linear.issues.update")

    assert input.authority.for_action_ref == "action://agent-loop/turn-1"

    assert_error_contains("action_ref mismatch", fn ->
      attrs
      |> Map.put(:action_ref, "action://agent-loop/other")
      |> AuthorizedInvocation.new!()
    end)

    assert_error_contains("for_action_ref mismatch", fn ->
      attrs
      |> put_in([:invocation_request, :execution_governance, :extensions, "citadel"], %{
        "for_action_ref" => "action://agent-loop/other"
      })
      |> AuthorizedInvocation.new!()
    end)
  end

  test "authorized invocation builds governed Codex turn input from Citadel execution intent" do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :allowed_operations], [
        "codex.session.turn",
        "linear.comments.update"
      ])
      |> put_in([:invocation_request, :execution_governance, :operations], %{
        "allowed_operations" => ["codex.session.turn", "linear.comments.update"]
      })
      |> put_in([:invocation_request, :execution_governance, :sandbox], %{
        "allowed_tools" => ["codex.session.turn", "linear.api.comments.update"],
        "file_scope_hint" => "/home/dev/extravaganza",
        "file_scope_ref" => "workspace://work_object/subject-1"
      })
      |> put_in([:invocation_request, :extensions, "citadel", "execution_intent"], %{
        "prompt" => "Implement the governed slice",
        "cwd" => "/home/dev/extravaganza",
        "continuation" => %{"strategy" => "latest"},
        "provider_metadata" => %{"model" => "gpt-5.4", "app_server" => true},
        "dynamic_tool_manifest" => %{"tools" => ["linear.comment.update"]},
        "host_tools" => [
          %{
            "name" => "linear_comment_update",
            "inputSchema" => %{"type" => "object"}
          }
        ]
      })

    invocation = AuthorizedInvocation.new!(attrs)
    input = AuthorizedInvocation.invoke_input(invocation, "codex.session.turn")

    assert input.prompt == "Implement the governed slice"
    assert input.cwd == "/home/dev/extravaganza"
    assert input.continuation == %{"strategy" => "latest"}

    assert input.host_tools == [
             %{"name" => "linear_comment_update", "inputSchema" => %{"type" => "object"}}
           ]

    assert input.provider_metadata["model"] == "gpt-5.4"
    assert input.provider_metadata["app_server"] == true

    assert input.provider_metadata["dynamic_tool_manifest"] == %{
             "tools" => ["linear.comment.update"]
           }

    assert input.dynamic_tool_manifest == %{"tools" => ["linear.comment.update"]}
    assert input.authority_metadata["authority_ref"] == "authority-decision://mock-decision-123"
    assert input.authority_metadata["authority_decision_hash"] == String.duplicate("a", 64)

    assert input.authority_metadata["allowed_operations"] == [
             "codex.session.turn",
             "linear.comments.update"
           ]
  end

  defp assert_error_contains(fragment, fun) do
    error = assert_raise(ArgumentError, fun)

    assert Exception.message(error) |> String.contains?(fragment)
  end

  test "authorized invocation preserves the older M1 per-execution authority path" do
    assert %AuthorizedInvocation{} = AuthorizedInvocation.new!(authorized_invocation_attrs())
  end

  test "dispatch_read routes generic lower reads through lineage-owned lower facts" do
    store_lineage!()

    intent =
      ReadIntent.new!(%{
        intent_id: "read-1",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{
          operation: :fetch_run
        }
      })

    assert {:ok, result} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    assert result.operation == :fetch_run
    assert result.source == :lower_run_status
    assert result.staleness_class == :lower_fresh
    refute result.operator_actionable?

    assert result.lineage == %{
             execution_id: "exec-1",
             installation_id: "inst-1",
             subject_id: "subject-1",
             trace_id: "trace-1"
           }

    assert result.result.run_id == "run-1"

    assert_received {:fetch_run,
                     [
                       %TenantScope{tenant_id: "tenant-1", installation_id: "inst-1"},
                       "run-1"
                     ]}
  end

  test "dispatch_read passes tenant scope to the substrate read slice and fails closed on mismatch" do
    store_lineage!(tenant_id: "tenant-other")

    Process.put(:integration_bridge_test_responses, %{
      fetch_run: fn [%TenantScope{tenant_id: "tenant-other"}, _run_id] ->
        {:error, :tenant_mismatch}
      end
    })

    intent =
      ReadIntent.new!(%{
        intent_id: "read-tenant-mismatch",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-other",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{operation: :fetch_run}
      })

    assert {:error, :tenant_mismatch} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    assert_received {:fetch_run,
                     [
                       %TenantScope{tenant_id: "tenant-other", installation_id: "inst-1"},
                       "run-1"
                     ]}
  end

  test "dispatch_read rejects missing execution lineage before lower fact access" do
    intent =
      ReadIntent.new!(%{
        intent_id: "read-missing-lineage",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          execution_id: "exec-missing"
        },
        query: %{operation: :fetch_run}
      })

    assert {:error, :unknown_execution_lineage} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    refute_received {:fetch_run, _args}
  end

  test "dispatch_read denies lower reads when installation context does not match the stored lineage" do
    store_lineage!()

    intent =
      ReadIntent.new!(%{
        intent_id: "read-2",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-2",
          execution_id: "exec-1"
        },
        query: %{operation: :fetch_run}
      })

    assert {:error, :unauthorized_lower_read} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    refute_received {:fetch_run, _args}
  end

  test "dispatch_read denies cross-tenant lineage reuse before lower fact access" do
    store_lineage!(tenant_id: "tenant-1")

    intent =
      ReadIntent.new!(%{
        intent_id: "read-cross-tenant-lineage",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-2",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{operation: :fetch_run}
      })

    assert {:error, :unauthorized_lower_read} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    refute_received {:fetch_run, _args}
  end

  test "dispatch_read forbids caller supplied lower run ids from becoming the primary lookup surface" do
    intent =
      ReadIntent.new!(%{
        intent_id: "read-3",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{
          operation: :fetch_run,
          run_id: "run-override"
        }
      })

    assert {:error, {:lower_identifier_override_forbidden, :run_id}} =
             IntegrationBridge.dispatch_read(intent,
               lower_facts: LowerFactsStub,
               fetch_lineage: fn _execution_id ->
                 send(self(), :lineage_fetch_called)
                 {:error, :should_not_fetch}
               end
             )

    refute_received :lineage_fetch_called
    refute_received {:fetch_run, _args}
  end

  test "dispatch_read rejects mismatched lower artifacts even after authorization succeeds" do
    store_lineage!()

    Process.put(:integration_bridge_test_responses, %{
      fetch_artifact: fn [%TenantScope{}, artifact_id] ->
        {:ok, %{artifact_id: artifact_id, run_id: "run-other"}}
      end
    })

    intent =
      ReadIntent.new!(%{
        intent_id: "read-4",
        read_type: :lower_fact,
        subject: %{
          actor_id: "actor-1",
          tenant_id: "tenant-1",
          installation_id: "inst-1",
          execution_id: "exec-1"
        },
        query: %{
          operation: :fetch_artifact,
          artifact_id: "artifact-1"
        }
      })

    assert {:error,
            {:mismatched_lower_fact,
             %{
               operation: :fetch_artifact,
               field: :run_id,
               expected: "run-1",
               actual: "run-other"
             }}} =
             IntegrationBridge.dispatch_read(intent, lower_facts: LowerFactsStub)

    assert_received {:fetch_artifact, [%TenantScope{tenant_id: "tenant-1"}, "artifact-1"]}
  end

  test "event translation maps direct platform outcomes to audit attrs" do
    mapped =
      IntegrationBridge.to_audit_attrs(
        %{status: :started, run_id: "run-1", payload: %{"attempt" => 1}},
        %{program_id: "program-1", work_object_id: "work-1"}
      )

    assert mapped.event_kind == :run_started
    assert mapped.program_id == "program-1"
    assert mapped.work_object_id == "work-1"
  end

  defp store_lineage!(opts \\ []) do
    lineage =
      ExecutionLineage.new!(%{
        trace_id: "trace-1",
        tenant_id: Keyword.get(opts, :tenant_id, "tenant-1"),
        installation_id: "inst-1",
        subject_id: "subject-1",
        execution_id: "exec-1",
        ji_submission_key: "submission-1",
        lower_run_id: "run-1",
        lower_attempt_id: "attempt-1",
        artifact_refs: ["artifact-1"]
      })

    assert {:ok, _stored} = ExecutionLineageStore.store(lineage)
  end

  defp authorized_invocation do
    AuthorizedInvocation.new!(authorized_invocation_attrs())
  end

  defp authorized_invocation_allowing(allowed_operations) do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :allowed_operations], allowed_operations)
      |> put_in([:invocation_request, :execution_governance, :operations], %{
        "allowed_operations" => allowed_operations
      })

    AuthorizedInvocation.new!(attrs)
  end

  defp authorized_invocation_with_governance_posture do
    attrs =
      authorized_invocation_attrs()
      |> put_in([:invocation_request, :execution_governance, :sandbox], %{
        "level" => "strict",
        "egress" => "restricted",
        "approvals" => "manual",
        "acceptable_attestation" => ["attestation://required"],
        "allowed_tools" => ["linear.issues.update"],
        "file_scope_ref" => "workspace://work_object/subject-1",
        "file_scope_hint" => nil
      })

    AuthorizedInvocation.new!(attrs)
  end

  defp authorized_invocation_attrs do
    %{
      tenant_id: "tenant-1",
      installation_id: "inst-1",
      subject_id: "subject-1",
      execution_id: "exec-1",
      trace_id: "trace-1",
      idempotency_key: "idem-1",
      submission_dedupe_key: "dedupe-1",
      invocation_request: invocation_request()
    }
  end

  defp invocation_request do
    %{
      schema_version: 2,
      invocation_request_id: "invoke-1",
      request_id: "request-1",
      session_id: "session-1",
      tenant_id: "tenant-1",
      trace_id: "trace-1",
      actor_id: "actor-1",
      target_id: "target-1",
      target_kind: "runtime_target",
      selected_step_id: "step-1",
      allowed_operations: ["linear.issues.retrieve", "linear.issues.update"],
      authority_packet: authority_packet(),
      boundary_intent: %{},
      topology_intent: %{},
      execution_governance: execution_governance(),
      extensions: %{
        "citadel" => %{
          "execution_envelope" => %{
            "installation_id" => "inst-1",
            "installation_revision" => 3,
            "subject_id" => "subject-1",
            "execution_id" => "exec-1",
            "submission_dedupe_key" => "dedupe-1"
          }
        }
      }
    }
  end

  defp authority_packet do
    %{
      contract_version: "v1",
      decision_id: "mock-decision-123",
      tenant_id: "tenant-1",
      request_id: "request-1",
      policy_version: "mock-v1",
      boundary_class: "workspace_session",
      trust_profile: "baseline",
      approval_profile: "standard",
      egress_profile: "restricted",
      workspace_profile: "workspace",
      resource_profile: "standard",
      decision_hash: String.duplicate("a", 64),
      extensions: %{"citadel" => %{}}
    }
  end

  defp execution_governance do
    %{
      contract_version: "v1",
      execution_governance_id: "mock-governance-123",
      authority_ref: %{"decision_id" => "mock-decision-123"},
      sandbox: %{"allowed_tools" => ["linear.issues.update"]},
      boundary: %{},
      topology: %{},
      workspace: %{},
      resources: %{},
      placement: %{},
      operations: %{"allowed_operations" => ["linear.issues.retrieve", "linear.issues.update"]},
      extensions: %{"citadel" => %{}}
    }
  end

  defp source_binding do
    %{
      source_binding_id: "linear-primary",
      installation_id: "inst-1",
      provider: "linear",
      connection_ref: "linear-primary",
      candidate_filters: %{project_slug: "ops-automation", assignee: "me"},
      state_mapping: %{
        "submitted" => ["Todo", "Backlog"],
        "retry_submission" => ["Todo"],
        "completed" => ["Done", "Completed"],
        "rejected" => ["Canceled", "Duplicate"]
      }
    }
  end

  defp linear_issue do
    %{
      id: "lin-issue-321",
      identifier: "ENG-321",
      title: "Investigate source publication",
      description: "Keep workpad in sync",
      priority: 2,
      labels: ["Automation"],
      branch_name: "eng-321-source-publication",
      url: "https://linear.app/acme/issue/ENG-321",
      created_at: "2026-03-12T09:15:00Z",
      updated_at: "2026-03-12T10:00:00Z",
      state: %{id: "state-todo", name: "Todo", type: "unstarted"},
      assignee: %{id: "usr-linear-viewer", name: "Taylor Automation"},
      blockers: []
    }
  end

  defp github_pr do
    %{
      repo: "nshkrdotcom/extravaganza",
      pull_number: 17,
      title: "Governed GitHub PR",
      state: "open",
      html_url: "https://github.com/nshkrdotcom/extravaganza/pull/17",
      head: %{ref: "phase-7", sha: "head-sha"},
      base: %{ref: "main", sha: "base-sha"}
    }
  end

  defp github_reviews do
    %{
      repo: "nshkrdotcom/extravaganza",
      pull_number: 17,
      reviews: [
        %{review_id: 1, state: "APPROVED"},
        %{review_id: 2, state: "CHANGES_REQUESTED"}
      ]
    }
  end

  defp github_review_comments do
    %{
      repo: "nshkrdotcom/extravaganza",
      pull_number: 17,
      comments: [%{comment_id: 11, path: "lib/extravaganza.ex"}]
    }
  end

  defp github_status do
    %{
      repo: "nshkrdotcom/extravaganza",
      ref: "head-sha",
      state: "success",
      statuses: [%{context: "mix ci", state: "success"}]
    }
  end

  defp github_checks do
    %{
      repo: "nshkrdotcom/extravaganza",
      ref: "head-sha",
      check_runs: [%{name: "mix ci", status: "completed", conclusion: "success"}]
    }
  end
end
