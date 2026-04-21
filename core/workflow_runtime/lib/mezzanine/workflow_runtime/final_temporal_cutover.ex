defmodule Mezzanine.WorkflowRuntime.FinalTemporalCutover do
  @moduledoc """
  Final cutover registry for the Phase 4 Temporal/Oban split.

  Temporal owns durable orchestration. Oban remains only for local transactional
  outboxes, claim-check garbage collection, and bounded local work.
  """

  @retained_queues [:claim_check_gc, :workflow_signal_outbox, :workflow_start_outbox]

  @retired_workers [
    %{
      worker: "Mezzanine.ExecutionDispatchWorker",
      retired_queue: :dispatch,
      replacement:
        "Mezzanine.Workflows.ExecutionAttempt + Mezzanine.Activities.SubmitJidoLowerActivity",
      envelope: "Mezzanine.WorkflowExecutionLifecycleInput.v1"
    },
    %{
      worker: "Mezzanine.ExecutionReceiptWorker",
      retired_queue: :receipt,
      replacement: "Mezzanine.WorkflowRuntime.ExecutionLifecycleWorkflow.receipt_signal/1",
      envelope: "Mezzanine.WorkflowReceiptSignal.v1"
    },
    %{
      worker: "Mezzanine.ExecutionReconcileWorker",
      retired_queue: :reconcile,
      replacement: "Mezzanine.Activities.ReconcileLowerRun",
      envelope: "Mezzanine.WorkflowExecutionLifecycleInput.v1"
    },
    %{
      worker: "Mezzanine.JoinAdvanceWorker",
      retired_queue: :join,
      replacement: "Mezzanine.Workflows.JoinBarrier",
      envelope: "Mezzanine.WorkflowFanoutFanin.v1"
    },
    %{
      worker: "Mezzanine.LifecycleContinuationWorker",
      retired_queue: :lifecycle,
      replacement: "Mezzanine.Workflows.ExecutionAttempt lifecycle transitions",
      envelope: "Mezzanine.WorkflowExecutionLifecycleInput.v1"
    },
    %{
      worker: "Mezzanine.ExecutionCancelWorker",
      retired_queue: :cancel,
      replacement: "Mezzanine.WorkflowRuntime.OperatorSignalControl operator.cancel",
      envelope: "Mezzanine.OperatorWorkflowSignal.v1"
    }
  ]

  @invalid_queues [:cancel, :decision_expiry, :dispatch, :join, :lifecycle, :receipt, :reconcile]
  @retired_worker_names Enum.map(@retired_workers, & &1.worker)
  @legacy_execution_dispatch_states [
    "pending_dispatch",
    "dispatching",
    "dispatching_retry",
    "awaiting_receipt",
    "running"
  ]

  @retired_worker_registry_files [
    "/core/workflow_runtime/lib/mezzanine/workflow_runtime/final_temporal_cutover.ex",
    "/core/workflow_runtime/lib/mezzanine/workflow_runtime/durable_orchestration_decision.ex"
  ]

  @doc "Final cutover manifest shape consumed by Stack Lab Scenario 104."
  @spec manifest() :: map()
  def manifest do
    %{
      contract_name: "Mezzanine.FinalTemporalCutoverManifest.v1",
      oban_scope_contract: "Mezzanine.ObanTemporalScope.v1",
      temporal_boundary: Mezzanine.WorkflowRuntime,
      temporalex_boundary: Mezzanine.WorkflowRuntime.TemporalexBoundary,
      temporal_integration_mode: :direct_temporalex_beam_workers,
      retained_oban_queues: @retained_queues,
      retained_oban_workers: [
        "Mezzanine.WorkflowRuntime.ClaimCheckGcWorker",
        "Mezzanine.WorkflowRuntime.WorkflowSignalOutboxWorker",
        "Mezzanine.WorkflowRuntime.WorkflowStarterOutboxWorker"
      ],
      retired_oban_saga_workers: @retired_workers,
      required_runtime_refs: [
        "Temporalex local relative path dependency",
        "Temporal Rust Core via temporalex Rustler NIF",
        "Mezzanine.WorkflowRuntime facade",
        "workflow/activity/signal/query versions",
        "Oban retained queue manifest",
        "Oban saga-removal source scan"
      ],
      release_manifest_ref: "phase4-v6-milestone31-temporal-cutover"
    }
  end

  @doc "Returns the active Oban worker modules found in Mezzanine source."
  @spec active_oban_worker_modules(Path.t() | String.t()) :: [String.t()]
  def active_oban_worker_modules(root) do
    root
    |> source_files("*.ex")
    |> Enum.flat_map(&discover_oban_worker_modules/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  @doc "Returns config files that still register retired saga queues."
  @spec invalid_oban_queue_configs(Path.t() | String.t()) :: [map()]
  def invalid_oban_queue_configs(root) do
    root
    |> source_files("*.exs")
    |> Enum.filter(&String.contains?(&1, "/config/"))
    |> Enum.flat_map(fn path ->
      text = File.read!(path)

      @invalid_queues
      |> Enum.filter(&Regex.match?(~r/queues:\s*\[[^\]]*\b#{&1}:/s, text))
      |> Enum.map(fn queue -> %{path: relative(root, path), queue: queue} end)
    end)
  end

  @doc "Returns source references that still enqueue or mutate Oban saga paths."
  @spec invalid_oban_saga_references(Path.t() | String.t()) :: [map()]
  def invalid_oban_saga_references(root) do
    root
    |> source_files("*.ex")
    |> Enum.reject(&excluded_source_file?/1)
    |> Enum.flat_map(fn path ->
      text = File.read!(path)

      invalid_fragments()
      |> Enum.filter(fn {_name, fragment} -> String.contains?(text, fragment) end)
      |> Enum.map(fn {name, _fragment} -> %{path: relative(root, path), reference: name} end)
    end)
  end

  @doc "Returns retired worker module definitions that still exist in runtime source."
  @spec retired_worker_module_definitions(Path.t() | String.t()) :: [map()]
  def retired_worker_module_definitions(root) do
    root
    |> source_files("*.ex")
    |> Enum.reject(&retired_worker_registry_file?/1)
    |> Enum.flat_map(fn path ->
      text = File.read!(path)

      @retired_worker_names
      |> Enum.filter(&Regex.match?(~r/defmodule\s+#{Regex.escape(&1)}\s+do/, text))
      |> Enum.map(fn worker -> %{path: relative(root, path), module: worker} end)
    end)
  end

  @doc "Returns non-registry source references to retired Oban saga worker modules."
  @spec retired_worker_runtime_references(Path.t() | String.t()) :: [map()]
  def retired_worker_runtime_references(root) do
    root
    |> source_files("*.ex")
    |> Enum.reject(&retired_worker_registry_file?/1)
    |> Enum.flat_map(fn path ->
      text = File.read!(path)

      @retired_worker_names
      |> Enum.filter(&String.contains?(text, &1))
      |> Enum.map(fn worker -> %{path: relative(root, path), module: worker} end)
    end)
  end

  @doc "Returns direct source writes that still emit legacy execution dispatch states."
  @spec legacy_execution_dispatch_state_write_references(Path.t() | String.t()) :: [map()]
  def legacy_execution_dispatch_state_write_references(root) do
    root
    |> source_files("*.ex")
    |> Enum.filter(&dispatch_state_write_source_file?/1)
    |> Enum.flat_map(fn path ->
      path
      |> source_lines()
      |> Enum.flat_map(&legacy_dispatch_state_line_references(root, path, &1))
    end)
  end

  @doc "Returns direct Temporalex references outside the workflow-runtime boundary."
  @spec temporalex_boundary_violations(Path.t() | String.t()) :: [map()]
  def temporalex_boundary_violations(root) do
    root
    |> source_files("*.ex")
    |> Enum.reject(&workflow_runtime_source_file?/1)
    |> Enum.flat_map(fn path ->
      path
      |> source_lines()
      |> Enum.flat_map(&temporalex_line_reference(root, path, &1))
    end)
  end

  @doc "Workers retired by the big-bang Temporal cutover."
  @spec retired_oban_saga_workers() :: [map()]
  def retired_oban_saga_workers, do: @retired_workers

  defp source_files(root, extension) do
    root = Path.expand(root)

    [
      Path.join([root, "core", "**", extension]),
      Path.join([root, "apps", "**", extension]),
      Path.join([root, "bridges", "**", extension]),
      Path.join([root, "config", extension])
    ]
    |> Enum.flat_map(&Path.wildcard/1)
    |> Enum.uniq()
    |> Enum.reject(&(String.contains?(&1, "/deps/") or String.contains?(&1, "/_build/")))
    |> Enum.sort()
  end

  defp discover_oban_worker_modules(path) do
    Regex.scan(
      ~r/defmodule\s+([A-Za-z0-9_.]+)\s+do(?:(?!\ndefmodule\s).)*?use\s+Oban\.Worker/s,
      File.read!(path)
    )
    |> Enum.map(fn [_match, module] -> module end)
  end

  defp source_lines(path) do
    path
    |> File.read!()
    |> String.split("\n")
    |> Enum.with_index(1)
  end

  defp legacy_dispatch_state_line_references(root, path, {line, line_number}) do
    if String.contains?(line, "dispatch_state") do
      @legacy_execution_dispatch_states
      |> Enum.filter(&line_contains_state?(line, &1))
      |> Enum.map(&legacy_dispatch_state_reference(root, path, line_number, &1))
    else
      []
    end
  end

  defp legacy_dispatch_state_reference(root, path, line_number, state) do
    %{path: relative(root, path), line: line_number, state: state}
  end

  defp temporalex_line_reference(root, path, {line, line_number}) do
    if String.contains?(line, "Temporalex") do
      [%{path: relative(root, path), line: line_number}]
    else
      []
    end
  end

  defp invalid_fragments do
    [
      {:direct_oban_row_mutation, "UPDATE oban_jobs"},
      {:dispatch_queue_enqueue, "JobOutbox.enqueue(\n           :dispatch"},
      {:receipt_queue_enqueue, "JobOutbox.enqueue(\n           :receipt"},
      {:reconcile_queue_enqueue, "JobOutbox.enqueue(\n           :reconcile"},
      {:join_queue_enqueue, "JobOutbox.enqueue(\n             :join"},
      {:cancel_queue_enqueue, "JobOutbox.enqueue(\n             @cancel_queue"}
    ]
  end

  defp excluded_source_file?(path) do
    String.contains?(path, "/test/") or
      retired_worker_registry_file?(path)
  end

  defp retired_worker_registry_file?(path) do
    Enum.any?(@retired_worker_registry_files, &String.ends_with?(path, &1))
  end

  defp dispatch_state_write_source_file?(path) do
    Enum.any?(
      [
        "/core/execution_engine/lib/",
        "/core/lifecycle_engine/lib/",
        "/core/operator_engine/lib/",
        "/core/runtime_scheduler/lib/"
      ],
      &String.contains?(path, &1)
    )
  end

  defp workflow_runtime_source_file?(path),
    do: String.contains?(path, "/core/workflow_runtime/lib/")

  defp line_contains_state?(line, state) do
    Regex.match?(~r/(^|[^A-Za-z0-9_])#{Regex.escape(state)}([^A-Za-z0-9_]|$)/, line)
  end

  defp relative(root, path) do
    path
    |> Path.relative_to(Path.expand(root))
    |> Path.split()
    |> Enum.join("/")
  end
end
