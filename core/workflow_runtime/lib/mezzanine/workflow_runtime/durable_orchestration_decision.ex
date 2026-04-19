defmodule Mezzanine.WorkflowRuntime.DurableOrchestrationDecision do
  @moduledoc """
  Phase 4 durable orchestration decision registry.

  This module is deliberately declarative. It gives Stack Lab and release
  manifests one source of truth for the Temporalex runtime posture, workflow
  identity rules, search-attribute registry, retained Oban scope, and Oban
  orchestration classes that must be replaced by Temporal workflows during the
  cutover milestones.
  """

  @integration_mode :direct_temporalex_beam_workers
  @temporalex_root "/home/home/p/g/n/temporalex"
  @temporal_endpoint "127.0.0.1:7233"
  @temporal_namespace "default"

  @workflow_types [
    %{
      name: :agent_run,
      module: Mezzanine.Workflows.AgentRun,
      task_queue: "mezzanine.agentic",
      version: "agent-run.v1",
      workflow_id_format: "tenant:<tenant_ref>:resource:<resource_ref>:agent-run:<command_id>"
    },
    %{
      name: :execution_attempt,
      module: Mezzanine.Workflows.ExecutionAttempt,
      task_queue: "mezzanine.hazmat",
      version: "execution-attempt.v1",
      workflow_id_format: "tenant:<tenant_ref>:execution:<execution_ref>:attempt:<attempt_no>"
    },
    %{
      name: :decision_review,
      module: Mezzanine.Workflows.DecisionReview,
      task_queue: "mezzanine.review",
      version: "decision-review.v1",
      workflow_id_format: "tenant:<tenant_ref>:review:<review_task_ref>"
    },
    %{
      name: :join_barrier,
      module: Mezzanine.Workflows.JoinBarrier,
      task_queue: "mezzanine.agentic",
      version: "join-barrier.v1",
      workflow_id_format: "tenant:<tenant_ref>:barrier:<barrier_ref>"
    },
    %{
      name: :incident_reconstruction,
      module: Mezzanine.Workflows.IncidentReconstruction,
      task_queue: "mezzanine.agentic",
      version: "incident-reconstruction.v1",
      workflow_id_format: "tenant:<tenant_ref>:incident:<incident_ref>"
    }
  ]

  @activity_registrations [
    %{
      name: :start_lower_execution,
      module: Mezzanine.Activities.StartLowerExecution,
      owner_repo: :execution_plane,
      task_queue: "mezzanine.hazmat",
      lease_broker?: true,
      version: "start-lower-execution.v1"
    },
    %{
      name: :record_evidence,
      module: Mezzanine.Activities.RecordEvidence,
      owner_repo: :mezzanine,
      task_queue: "mezzanine.agentic",
      lease_broker?: false,
      version: "record-evidence.v1"
    },
    %{
      name: :request_decision,
      module: Mezzanine.Activities.RequestDecision,
      owner_repo: :citadel,
      task_queue: "mezzanine.agentic",
      lease_broker?: false,
      version: "request-decision.v1"
    },
    %{
      name: :call_outer_brain,
      module: Mezzanine.Activities.CallOuterBrain,
      owner_repo: :outer_brain,
      task_queue: "mezzanine.semantic",
      lease_broker?: false,
      version: "call-outer-brain.v1"
    },
    %{
      name: :reconcile_lower_run,
      module: Mezzanine.Activities.ReconcileLowerRun,
      owner_repo: :jido_integration,
      task_queue: "mezzanine.agentic",
      lease_broker?: true,
      version: "reconcile-lower-run.v1"
    },
    %{
      name: :compensate_cancelled_run,
      module: Mezzanine.Activities.CompensateCancelledRun,
      owner_repo: :execution_plane,
      task_queue: "mezzanine.hazmat",
      lease_broker?: true,
      version: "compensate-cancelled-run.v1"
    }
  ]

  @search_attribute_registry [
    %{key: "phase4.tenant_ref", type: :keyword, redaction: :public_safe_ref},
    %{key: "phase4.resource_ref", type: :keyword, redaction: :public_safe_ref},
    %{key: "phase4.workflow_type", type: :keyword, redaction: :public_safe_ref},
    %{key: "phase4.workflow_version", type: :keyword, redaction: :public_safe_ref},
    %{key: "phase4.command_id", type: :keyword, redaction: :public_safe_ref},
    %{key: "phase4.trace_id", type: :keyword, redaction: :public_safe_ref},
    %{key: "phase4.idempotency_key_hash", type: :keyword, redaction: :sha256_only},
    %{key: "phase4.review_required", type: :bool, redaction: :scalar},
    %{key: "phase4.semantic_score", type: :double, redaction: :scalar},
    %{key: "phase4.risk_band", type: :keyword, redaction: :scalar},
    %{key: "phase4.retry_class", type: :keyword, redaction: :scalar},
    %{key: "phase4.release_manifest_ref", type: :keyword, redaction: :public_safe_ref}
  ]

  @allowed_search_attribute_types [:keyword, :bool, :int, :double, :datetime, :keyword_list]

  @retained_oban_roles [
    %{role: :workflow_start_outbox, queue: :workflow_start_outbox, classification: :valid_outbox},
    %{
      role: :workflow_signal_outbox,
      queue: :workflow_signal_outbox,
      classification: :valid_outbox
    },
    %{role: :claim_check_gc, queue: :claim_check_gc, classification: :valid_claim_check_gc},
    %{role: :bounded_local_job, queue: :decision_expiry, classification: :valid_bounded_local_job}
  ]

  @oban_scope [
    %{
      worker: Mezzanine.DecisionExpiryWorker,
      queue: :decision_expiry,
      classification: :valid_bounded_local_job,
      reason: "short local decision expiry, not a multi-step saga"
    },
    %{
      worker: Mezzanine.ExecutionDispatchWorker,
      queue: :dispatch,
      classification: :invalid_saga_orchestration,
      replacement_milestone: 31,
      reason: "execution dispatch becomes Temporal activity/workflow control"
    },
    %{
      worker: Mezzanine.ExecutionReceiptWorker,
      queue: :receipt,
      classification: :invalid_saga_orchestration,
      replacement_milestone: 31,
      reason: "execution receipt continuation becomes workflow activity result handling"
    },
    %{
      worker: Mezzanine.ExecutionReconcileWorker,
      queue: :reconcile,
      classification: :invalid_saga_orchestration,
      replacement_milestone: 31,
      reason: "long-running reconciliation belongs in Temporal workflow state"
    },
    %{
      worker: Mezzanine.JoinAdvanceWorker,
      queue: :join,
      classification: :invalid_saga_orchestration,
      replacement_milestone: 31,
      reason: "join barrier advancement becomes Temporal fan-in state"
    },
    %{
      worker: Mezzanine.LifecycleContinuationWorker,
      queue: :lifecycle,
      classification: :invalid_saga_orchestration,
      replacement_milestone: 31,
      reason: "post-commit lifecycle continuations become workflow transitions"
    },
    %{
      worker: Mezzanine.ExecutionCancelWorker,
      queue: :cancel,
      classification: :invalid_saga_orchestration,
      replacement_milestone: 31,
      reason: "operator cancel becomes authorized Temporal signal dispatch"
    }
  ]

  @workflow_history_allowed [
    :tenant_ref,
    :command_id,
    :workflow_id,
    :resource_ref,
    :trace_id,
    :activity_result_refs,
    :lower_refs,
    :semantic_refs,
    :review_refs,
    :projection_refs,
    :routing_facts,
    :status_summary
  ]

  @workflow_history_forbidden [
    :raw_prompt,
    :raw_provider_body,
    :raw_context_pack,
    :raw_artifact,
    :raw_lower_log,
    :authority_packet_body,
    :claim_check_body,
    :temporal_protobuf,
    :temporalex_struct,
    :nif_resource,
    :task_token,
    :raw_history_event
  ]

  @routing_fact_fields [
    :review_required,
    :semantic_score,
    :confidence_band,
    :risk_band,
    :schema_validation_state,
    :normalization_warning_count,
    :semantic_retry_class,
    :terminal_class,
    :next_step
  ]

  @doc "Direct Temporalex integration mode selected for Phase 4."
  @spec integration_mode() :: atom()
  def integration_mode, do: @integration_mode

  @doc "Local Temporalex source root recorded by the M25 decision."
  @spec temporalex_root() :: String.t()
  def temporalex_root, do: @temporalex_root

  @doc "Temporal runtime endpoint and namespace for local dev proof posture."
  @spec runtime_refs() :: map()
  def runtime_refs do
    %{
      endpoint: @temporal_endpoint,
      namespace: @temporal_namespace,
      source_root: @temporalex_root,
      rust_core_posture: "Temporal Rust Core via temporalex Rustler NIF",
      nif_posture: "Mezzanine runtime-only dependency; public DTOs never expose NIF resources"
    }
  end

  @doc "Consuming mix.exs files and their relative Temporalex path dependency."
  @spec temporalex_dependency_paths() :: [map()]
  def temporalex_dependency_paths do
    [
      %{
        mix_exs: "mix.exs",
        from_dir: "/home/home/p/g/n/mezzanine",
        dependency: {:temporalex, path: "../temporalex"}
      },
      %{
        mix_exs: "core/workflow_runtime/mix.exs",
        from_dir: "/home/home/p/g/n/mezzanine/core/workflow_runtime",
        dependency: {:temporalex, path: "../../../temporalex"}
      }
    ]
  end

  @doc "Final Phase 4 workflow type registry."
  @spec workflow_types() :: [map()]
  def workflow_types, do: @workflow_types

  @doc "Final Phase 4 activity registration registry."
  @spec activity_registrations() :: [map()]
  def activity_registrations, do: @activity_registrations

  @doc "Workflow task queues selected for Phase 4."
  @spec task_queues() :: [String.t()]
  def task_queues do
    @workflow_types
    |> Enum.map(& &1.task_queue)
    |> Kernel.++(Enum.map(@activity_registrations, & &1.task_queue))
    |> Enum.uniq()
    |> Enum.sort()
  end

  @doc "Temporal Search Attribute registry. Values must stay scalar."
  @spec search_attribute_registry() :: [map()]
  def search_attribute_registry, do: @search_attribute_registry

  @doc "Whether a Search Attribute value is a registry-allowed scalar."
  @spec scalar_search_attribute_value?(term()) :: boolean()
  def scalar_search_attribute_value?(value)
      when is_binary(value) or is_boolean(value) or is_integer(value) or is_float(value),
      do: true

  def scalar_search_attribute_value?(%DateTime{}), do: true

  def scalar_search_attribute_value?(values) when is_list(values),
    do: Enum.all?(values, &is_binary/1)

  def scalar_search_attribute_value?(_value), do: false

  @doc "Registry-approved scalar value types."
  @spec allowed_search_attribute_types() :: [atom()]
  def allowed_search_attribute_types, do: @allowed_search_attribute_types

  @doc "Oban roles that remain valid after Temporal cutover."
  @spec retained_oban_roles() :: [map()]
  def retained_oban_roles, do: @retained_oban_roles

  @doc "Every known Oban worker class and its Phase 4 classification."
  @spec oban_scope() :: [map()]
  def oban_scope, do: @oban_scope

  @doc "Classifications that are valid permanent Oban uses."
  @spec valid_oban_classifications() :: [atom()]
  def valid_oban_classifications do
    [:valid_outbox, :valid_claim_check_gc, :valid_bounded_local_job]
  end

  @doc "Classifications that must be eliminated by the cutover milestone."
  @spec cutover_oban_classifications() :: [atom()]
  def cutover_oban_classifications, do: [:invalid_saga_orchestration]

  @doc "Workflow history payload policy."
  @spec workflow_history_policy() :: map()
  def workflow_history_policy do
    %{
      allowed: @workflow_history_allowed,
      forbidden: @workflow_history_forbidden,
      routing_fact_fields: @routing_fact_fields
    }
  end

  @doc "Source-boundary rules used by Stack Lab scans."
  @spec source_boundary() :: map()
  def source_boundary do
    %{
      temporalex_allowed_repos: [:mezzanine],
      temporalex_allowed_namespaces: [
        "Mezzanine.WorkflowRuntime",
        "Mezzanine.Workflows",
        "Mezzanine.Activities"
      ],
      temporalex_allowed_paths: [
        "mix.exs",
        "core/workflow_runtime/mix.exs",
        "core/workflow_runtime/lib/mezzanine/workflow_runtime/durable_orchestration_decision.ex",
        "core/workflow_runtime/test/mezzanine/workflow_runtime/durable_orchestration_decision_test.exs"
      ],
      public_dto_forbidden_fragments: Enum.map(@workflow_history_forbidden, &Atom.to_string/1)
    }
  end

  @doc "Returns true when the decision registry is internally complete."
  @spec complete?() :: boolean()
  def complete? do
    [
      integration_mode() == :direct_temporalex_beam_workers,
      length(workflow_types()) == 5,
      length(activity_registrations()) >= 6,
      Enum.all?(search_attribute_registry(), &(&1.type in allowed_search_attribute_types())),
      Enum.any?(oban_scope(), &(&1.classification == :invalid_saga_orchestration))
    ]
    |> Enum.all?()
  end
end

defmodule Mezzanine.WorkflowRuntime.TemporalRegistry do
  @moduledoc """
  Temporalex worker registration registry for Phase 4 Mezzanine workflows.
  """

  alias Mezzanine.WorkflowRuntime.DurableOrchestrationDecision

  @spec workflows() :: [module()]
  def workflows, do: Enum.map(DurableOrchestrationDecision.workflow_types(), & &1.module)

  @spec activities() :: [module()]
  def activities, do: Enum.map(DurableOrchestrationDecision.activity_registrations(), & &1.module)

  @spec task_queues() :: [String.t()]
  def task_queues, do: DurableOrchestrationDecision.task_queues()
end

defmodule Mezzanine.WorkflowRuntime.TemporalexBoundary do
  @moduledoc """
  Internal-only Temporalex client boundary.

  Public callers use `Mezzanine.WorkflowRuntime` DTOs. This module is the
  only M25 source that maps those DTOs to `Temporalex.Client` calls.
  """

  @client Temporalex.Client

  @doc "Temporalex client module used by the Mezzanine runtime boundary."
  @spec client_module() :: module()
  def client_module, do: @client

  @doc "Temporalex client calls permitted at the Mezzanine runtime boundary."
  @spec client_calls() :: map()
  def client_calls do
    %{
      start_workflow: {@client, :start_workflow, 4},
      signal_workflow: {@client, :signal_workflow, 5},
      query_workflow: {@client, :query_workflow, 5},
      cancel_workflow: {@client, :cancel_workflow, 3},
      terminate_workflow: {@client, :terminate_workflow, 3},
      describe_workflow: {@client, :describe_workflow, 3},
      list_workflows: {@client, :list_workflows, 3}
    }
  end

  @spec start_workflow(term(), module(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def start_workflow(conn, workflow_module, args, opts \\ []) do
    conn
    |> @client.start_workflow(workflow_module, args, opts)
    |> normalize_result()
  end

  @spec signal_workflow(term(), String.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def signal_workflow(conn, workflow_id, signal_name, args, opts \\ []) do
    conn
    |> @client.signal_workflow(workflow_id, signal_name, args, opts)
    |> normalize_result()
  end

  @spec query_workflow(term(), String.t(), String.t(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def query_workflow(conn, workflow_id, query_name, args, opts \\ []) do
    conn
    |> @client.query_workflow(workflow_id, query_name, args, opts)
    |> normalize_result()
  end

  @spec cancel_workflow(term(), String.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def cancel_workflow(conn, workflow_id, opts \\ []) do
    conn
    |> @client.cancel_workflow(workflow_id, opts)
    |> normalize_result()
  end

  @spec terminate_workflow(term(), String.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def terminate_workflow(conn, workflow_id, opts \\ []) do
    conn
    |> @client.terminate_workflow(workflow_id, opts)
    |> normalize_result()
  end

  @spec describe_workflow(term(), String.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def describe_workflow(conn, workflow_id, opts \\ []) do
    conn
    |> @client.describe_workflow(workflow_id, opts)
    |> normalize_result()
  end

  @spec list_workflows(term(), String.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def list_workflows(conn, query, opts \\ []) do
    conn
    |> @client.list_workflows(query, opts)
    |> normalize_result()
  end

  defp normalize_result({:ok, value}), do: {:ok, value}
  defp normalize_result({:error, reason}), do: {:error, {:temporalex, reason}}
  defp normalize_result(value), do: {:ok, value}
end

defmodule Mezzanine.Workflows.AgentRun do
  @moduledoc "Phase 4 agent-run workflow skeleton."
  @behaviour Temporalex.Workflow

  alias Mezzanine.Workflows.Support

  @doc false
  def __workflow_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __workflow_defaults__, do: [task_queue: "mezzanine.agentic"]

  @impl Temporalex.Workflow
  def run(input), do: {:ok, Support.compact_result(:agent_run, input)}
end

defmodule Mezzanine.Workflows.ExecutionAttempt do
  @moduledoc "Phase 4 execution-attempt workflow skeleton."
  @behaviour Temporalex.Workflow

  alias Mezzanine.Workflows.Support

  @doc false
  def __workflow_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __workflow_defaults__, do: [task_queue: "mezzanine.hazmat"]

  @impl Temporalex.Workflow
  def run(input), do: {:ok, Support.compact_result(:execution_attempt, input)}
end

defmodule Mezzanine.Workflows.DecisionReview do
  @moduledoc "Phase 4 human decision-review workflow skeleton."
  @behaviour Temporalex.Workflow

  alias Mezzanine.Workflows.Support

  @doc false
  def __workflow_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __workflow_defaults__, do: [task_queue: "mezzanine.review"]

  @impl Temporalex.Workflow
  def run(input), do: {:ok, Support.compact_result(:decision_review, input)}
end

defmodule Mezzanine.Workflows.JoinBarrier do
  @moduledoc "Phase 4 fan-in join-barrier workflow skeleton."
  @behaviour Temporalex.Workflow

  alias Mezzanine.Workflows.Support

  @doc false
  def __workflow_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __workflow_defaults__, do: [task_queue: "mezzanine.agentic"]

  @impl Temporalex.Workflow
  def run(input), do: {:ok, Support.compact_result(:join_barrier, input)}
end

defmodule Mezzanine.Workflows.IncidentReconstruction do
  @moduledoc "Phase 4 incident-reconstruction workflow skeleton."
  @behaviour Temporalex.Workflow

  alias Mezzanine.Workflows.Support

  @doc false
  def __workflow_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __workflow_defaults__, do: [task_queue: "mezzanine.agentic"]

  @impl Temporalex.Workflow
  def run(input), do: {:ok, Support.compact_result(:incident_reconstruction, input)}
end

defmodule Mezzanine.Workflows.Support do
  @moduledoc false

  @spec compact_result(atom(), term()) :: map()
  def compact_result(workflow_type, input) do
    %{
      workflow_type: workflow_type,
      status: :accepted,
      trace_id: map_value(input, :trace_id),
      resource_ref: map_value(input, :resource_ref),
      routing_facts: map_value(input, :routing_facts, %{})
    }
  end

  defp map_value(input, key, default \\ nil)
  defp map_value(input, key, default) when is_map(input), do: Map.get(input, key, default)
  defp map_value(_input, _key, default), do: default
end

defmodule Mezzanine.Activities.StartLowerExecution do
  @moduledoc "Phase 4 lower-execution activity skeleton."
  @behaviour Temporalex.Activity

  alias Mezzanine.Activities.Support

  @doc false
  def __activity_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __activity_defaults__,
    do: [task_queue: "mezzanine.hazmat", start_to_close_timeout: :timer.seconds(30)]

  @impl Temporalex.Activity
  def perform(input), do: Support.compact_result(:start_lower_execution, input)

  @impl Temporalex.Activity
  def perform(_ctx, input), do: perform(input)
end

defmodule Mezzanine.Activities.RecordEvidence do
  @moduledoc "Phase 4 evidence-recording activity skeleton."
  @behaviour Temporalex.Activity

  alias Mezzanine.Activities.Support

  @doc false
  def __activity_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __activity_defaults__,
    do: [task_queue: "mezzanine.agentic", start_to_close_timeout: :timer.seconds(10)]

  @impl Temporalex.Activity
  def perform(input), do: Support.compact_result(:record_evidence, input)

  @impl Temporalex.Activity
  def perform(_ctx, input), do: perform(input)
end

defmodule Mezzanine.Activities.RequestDecision do
  @moduledoc "Phase 4 Citadel decision-request activity skeleton."
  @behaviour Temporalex.Activity

  alias Mezzanine.Activities.Support

  @doc false
  def __activity_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __activity_defaults__,
    do: [task_queue: "mezzanine.agentic", start_to_close_timeout: :timer.seconds(10)]

  @impl Temporalex.Activity
  def perform(input), do: Support.compact_result(:request_decision, input)

  @impl Temporalex.Activity
  def perform(_ctx, input), do: perform(input)
end

defmodule Mezzanine.Activities.CallOuterBrain do
  @moduledoc "Phase 4 semantic activity skeleton."
  @behaviour Temporalex.Activity

  alias Mezzanine.Activities.Support

  @doc false
  def __activity_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __activity_defaults__,
    do: [task_queue: "mezzanine.semantic", start_to_close_timeout: :timer.seconds(60)]

  @impl Temporalex.Activity
  def perform(input), do: Support.compact_result(:call_outer_brain, input)

  @impl Temporalex.Activity
  def perform(_ctx, input), do: perform(input)
end

defmodule Mezzanine.Activities.ReconcileLowerRun do
  @moduledoc "Phase 4 lower-run reconciliation activity skeleton."
  @behaviour Temporalex.Activity

  alias Mezzanine.Activities.Support

  @doc false
  def __activity_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __activity_defaults__,
    do: [task_queue: "mezzanine.agentic", start_to_close_timeout: :timer.seconds(30)]

  @impl Temporalex.Activity
  def perform(input), do: Support.compact_result(:reconcile_lower_run, input)

  @impl Temporalex.Activity
  def perform(_ctx, input), do: perform(input)
end

defmodule Mezzanine.Activities.CompensateCancelledRun do
  @moduledoc "Phase 4 cancellation-compensation activity skeleton."
  @behaviour Temporalex.Activity

  alias Mezzanine.Activities.Support

  @doc false
  def __activity_type__, do: __MODULE__ |> Module.split() |> Enum.join(".")

  @doc false
  def __activity_defaults__,
    do: [task_queue: "mezzanine.hazmat", start_to_close_timeout: :timer.seconds(30)]

  @impl Temporalex.Activity
  def perform(input), do: Support.compact_result(:compensate_cancelled_run, input)

  @impl Temporalex.Activity
  def perform(_ctx, input), do: perform(input)
end

defmodule Mezzanine.Activities.Support do
  @moduledoc false

  @spec compact_result(atom(), term()) :: {:ok, map()}
  def compact_result(activity, input) do
    {:ok,
     %{
       activity: activity,
       status: :accepted,
       result_ref: result_ref(activity, input),
       trace_id: map_value(input, :trace_id),
       routing_facts: map_value(input, :routing_facts, %{})
     }}
  end

  defp result_ref(activity, input) do
    case map_value(input, :result_ref) do
      nil -> "activity-result://mezzanine/#{activity}"
      result_ref -> result_ref
    end
  end

  defp map_value(input, key, default \\ nil)
  defp map_value(input, key, default) when is_map(input), do: Map.get(input, key, default)
  defp map_value(_input, _key, default), do: default
end
