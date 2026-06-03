defmodule Mezzanine.Workflow.ChassisDeploymentWorkflow do
  @moduledoc "Mezzanine workflow facade for Chassis deployment materialization."

  alias Chassis.AppRegistry
  alias Chassis.Boundary
  alias Chassis.Mezzanine.Bridge
  alias Chassis.Mezzanine.Bridge.Outbox, as: ChassisOutbox
  alias Chassis.Receipts.Store
  alias Chassis.StackManager.FenceStore
  alias Mezzanine.Outbox.ChassisDrainWorker
  alias Mezzanine.Read.ChassisDeploymentProjection

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])

  def dispatch(attrs, opts) when is_list(attrs), do: dispatch(Map.new(attrs), opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    resources = resources(opts)
    request = materialize_request(attrs)
    envelope = envelope_attrs(attrs, "materialize")

    bridge_opts =
      [
        boundary_dispatcher: Keyword.get(opts, :boundary_dispatcher, Chassis.Boundary),
        registry: resources.registry,
        receipts_store: resources.receipts_store,
        fence_store: resources.fence_store,
        outbox: resources.outbox,
        app_atom: Map.get(attrs, :app_atom, :demo)
      ]
      |> maybe_put(:authorize, Keyword.get(opts, :authorize))

    with {:ok, %Boundary.Envelope{} = response} <-
           Bridge.dispatch(:materialize_deployment, request, envelope, bridge_opts),
         {:ok, %{delivered: delivered}} <-
           ChassisDrainWorker.drain(resources.outbox,
             read_store: resources.read_store,
             projection_store: resources.projection_store
           ),
         {:ok, projection} <-
           ChassisDeploymentProjection.latest(resources.read_store,
             tenant_ref: Map.get(attrs, :tenant_ref, "tenant:dev"),
             installation_ref: Map.get(attrs, :installation_ref, "installation:acme:demo")
           ) do
      {:ok,
       %{
         workflow: :chassis_deployment,
         status: to_string(projection.status),
         deployment_receipt_ref: response.payload.deployment_receipt_ref,
         app_ref: response.payload.app_ref,
         projection_ref: projection.receipt_ref,
         outbox_delivered: delivered
       }}
    end
  end

  defp materialize_request(attrs) do
    %Boundary.MaterializeDeployment.Request{
      topology_ref: Map.get(attrs, :topology_ref, "topology:profile:monolith"),
      service_spec_ref: Map.get(attrs, :service_spec_ref, "service:demo"),
      runtime_profile_ref: Map.get(attrs, :runtime_profile_ref, "profile:monolith"),
      placement_ref: Map.get(attrs, :placement_ref, "placement:local"),
      environment: Map.get(attrs, :environment, :dev),
      git_sha: Map.get(attrs, :git_sha, "unknown"),
      release_version: Map.get(attrs, :release_version, "unknown")
    }
  end

  defp envelope_attrs(attrs, operation) do
    %{
      envelope_ref:
        Map.get(attrs, :envelope_ref, "env:mezzanine.chassis.#{operation}:#{unique()}"),
      tenant_ref: Map.get(attrs, :tenant_ref, "tenant:dev"),
      installation_ref: Map.get(attrs, :installation_ref, "installation:acme:demo"),
      actor_ref: Map.get(attrs, :actor_ref, "actor:mezzanine.workflow"),
      authority_ref: Map.get(attrs, :authority_ref, "authority:mezzanine:local"),
      idempotency_key:
        Map.get(attrs, :idempotency_key, "idem:mezzanine.chassis.#{operation}:#{unique()}"),
      trace_id: Map.get(attrs, :trace_id, "trace:mezzanine.chassis.#{operation}:#{unique()}")
    }
  end

  defp resources(opts) do
    %{
      receipts_store: Keyword.get_lazy(opts, :receipts_store, &start_receipts_store/0),
      registry: Keyword.get_lazy(opts, :registry, &start_registry/0),
      fence_store: Keyword.get_lazy(opts, :fence_store, &start_fence_store/0),
      outbox: Keyword.get_lazy(opts, :outbox, &start_outbox/0),
      read_store: Keyword.get_lazy(opts, :read_store, &start_read_store/0),
      projection_store:
        Keyword.get(opts, :projection_store, {:file, ChassisDeploymentProjection.default_file()})
    }
  end

  defp start_receipts_store do
    {:ok, pid} = Store.Memory.start_link(name: nil)
    pid
  end

  defp start_registry do
    {:ok, pid} = AppRegistry.start_link(name: nil)
    pid
  end

  defp start_fence_store do
    {:ok, pid} = FenceStore.start_link(name: nil)
    pid
  end

  defp start_outbox do
    {:ok, pid} = ChassisOutbox.start_link(name: nil)
    pid
  end

  defp start_read_store do
    {:ok, pid} = ChassisDeploymentProjection.start_link(name: nil)
    pid
  end

  defp maybe_put(keyword, _key, nil), do: keyword
  defp maybe_put(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end

defmodule Mezzanine.Workflow.ChassisRollbackWorkflow do
  @moduledoc "Mezzanine workflow facade for Chassis rollback."

  alias Chassis.AppRegistry
  alias Chassis.AppRegistry.Entry, as: AppEntry
  alias Chassis.Boundary
  alias Chassis.Mezzanine.Bridge
  alias Chassis.Receipts.{DeploymentRecord, Store}
  alias Chassis.StackManager.CheckpointStore

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])

  def dispatch(attrs, opts) when is_list(attrs), do: dispatch(Map.new(attrs), opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    resources = resources(opts)
    app_ref = Map.fetch!(attrs, :app_ref)
    current_ref = Map.get(attrs, :current_receipt_ref, "receipt:deployment:current:#{unique()}")
    target_ref = Map.get(attrs, :target_receipt_ref, "receipt:deployment:target:#{unique()}")

    with :ok <- seed_rollback_state(resources, attrs, app_ref, current_ref, target_ref),
         {:ok, %Boundary.Envelope{} = response} <-
           Bridge.dispatch(
             :rollback_deployment,
             rollback_request(attrs, current_ref),
             envelope_attrs(attrs),
             registry: resources.registry,
             receipts_store: resources.receipts_store,
             checkpoint_store: resources.checkpoint_store,
             app_ref: app_ref
           ) do
      {:ok,
       %{
         workflow: :chassis_rollback,
         status: response.payload.status,
         app_ref: app_ref,
         rollback_receipt_ref: response.payload.rollback_receipt_ref,
         restored_revision: response.payload.restored_revision
       }}
    end
  end

  defp rollback_request(attrs, current_ref) do
    %Boundary.RollbackDeployment.Request{
      deployment_receipt_ref: current_ref,
      rollback_ref: Map.get(attrs, :rollback_ref, "rollback:mezzanine:#{unique()}"),
      reason: Map.get(attrs, :reason, "operator"),
      target_revision: Map.get(attrs, :target_revision, "previous")
    }
  end

  defp envelope_attrs(attrs) do
    %{
      envelope_ref: Map.get(attrs, :envelope_ref, "env:mezzanine.chassis.rollback:#{unique()}"),
      tenant_ref: Map.get(attrs, :tenant_ref, "tenant:dev"),
      installation_ref: Map.get(attrs, :installation_ref, "installation:acme:demo"),
      actor_ref: Map.get(attrs, :actor_ref, "actor:mezzanine.workflow"),
      authority_ref: Map.get(attrs, :authority_ref, "authority:mezzanine:local"),
      idempotency_key:
        Map.get(attrs, :idempotency_key, "idem:mezzanine.chassis.rollback:#{unique()}"),
      trace_id: Map.get(attrs, :trace_id, "trace:mezzanine.chassis.rollback:#{unique()}")
    }
  end

  defp seed_rollback_state(resources, attrs, app_ref, current_ref, target_ref) do
    current = deployment_record(app_ref, current_ref, attrs)
    target = deployment_record(app_ref, target_ref, attrs)

    with {:ok, _current} <- Store.Memory.put(resources.receipts_store, current),
         {:ok, _target} <- Store.Memory.put(resources.receipts_store, target),
         {:ok, entry} <-
           AppEntry.new(%{
             app_ref: app_ref,
             app_atom: Map.get(attrs, :app_atom, :demo),
             installation_ref: Map.get(attrs, :installation_ref, "installation:acme:demo"),
             tenant_ref: Map.get(attrs, :tenant_ref, "tenant:dev"),
             active_profile: Map.get(attrs, :runtime_profile_ref, "profile:monolith"),
             environment: Map.get(attrs, :environment, :dev),
             git_sha: Map.get(attrs, :git_sha, "unknown"),
             release_version: Map.get(attrs, :release_version, "unknown"),
             node_mesh: [node()],
             status: :active,
             last_deployment_receipt_ref: current_ref,
             rollback_target_ref: target_ref
           }),
         {:ok, _entry} <- AppRegistry.register(resources.registry, entry) do
      :ok
    end
  end

  defp deployment_record(app_ref, receipt_ref, attrs) do
    %DeploymentRecord{
      receipt_ref: receipt_ref,
      app_ref: app_ref,
      profile_ref: Map.get(attrs, :runtime_profile_ref, "profile:monolith"),
      env: Map.get(attrs, :environment, :dev),
      status: :active,
      authority_ref: Map.get(attrs, :authority_ref, "authority:mezzanine:local"),
      tenant_ref: Map.get(attrs, :tenant_ref, "tenant:dev")
    }
  end

  defp resources(opts) do
    %{
      receipts_store: Keyword.get_lazy(opts, :receipts_store, &start_receipts_store/0),
      registry: Keyword.get_lazy(opts, :registry, &start_registry/0),
      checkpoint_store: Keyword.get_lazy(opts, :checkpoint_store, &start_checkpoint_store/0)
    }
  end

  defp start_receipts_store do
    {:ok, pid} = Store.Memory.start_link(name: nil)
    pid
  end

  defp start_registry do
    {:ok, pid} = AppRegistry.start_link(name: nil)
    pid
  end

  defp start_checkpoint_store do
    {:ok, pid} = CheckpointStore.start_link(name: nil)
    pid
  end

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end

defmodule Mezzanine.Workflow.Chassis.Evolution.Engine do
  @moduledoc "Shared execution helpers for Mezzanine-owned Chassis evolution workflows."

  alias Chassis.Boundary
  alias Chassis.Evolution.Receipts.Store.Memory, as: EvolutionReceiptStore
  alias Chassis.Mezzanine.Bridge
  alias Chassis.Mezzanine.Bridge.Evolution.LocalDispatcher
  alias Chassis.Mezzanine.Bridge.Outbox, as: ChassisOutbox
  alias Mezzanine.Outbox.ChassisDrainWorker
  alias Mezzanine.Read.ChassisEvolutionProjection

  @signals %{
    consent: "mezzanine.signal.chassis.evolution.consent.v1",
    stop: "mezzanine.signal.chassis.evolution.stop.v1",
    metabolic_rollback: "mezzanine.signal.chassis.evolution.metabolic_rollback.v1"
  }

  @spec signals() :: map()
  def signals, do: @signals

  @spec input_digest(map() | keyword()) :: String.t()
  def input_digest(attrs) do
    attrs
    |> normalize_attrs()
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @spec idempotency_key(atom() | String.t(), atom() | String.t(), map() | keyword()) ::
          String.t()
  def idempotency_key(workflow_id, step_id, attrs) do
    material = "#{workflow_id}||#{step_id}||#{input_digest(attrs)}"
    "idem:" <> (:crypto.hash(:sha256, material) |> Base.encode16(case: :lower))
  end

  @spec dispatch_step(atom(), atom(), atom(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def dispatch_step(operation, workflow_id, step_id, attrs, opts \\ []) when is_map(attrs) do
    resources = resources(opts)
    envelope = envelope_attrs(attrs, workflow_id, step_id)

    bridge_opts = [
      boundary_dispatcher: Keyword.get(opts, :boundary_dispatcher, LocalDispatcher),
      receipts_store: resources.receipts_store,
      outbox: resources.outbox
    ]

    with {:ok, %Boundary.Envelope{} = response} <-
           Bridge.dispatch(operation, request_attrs(attrs), envelope, bridge_opts),
         {:ok, %{delivered: delivered}} <-
           ChassisDrainWorker.drain(resources.outbox,
             read_store: resources.read_store,
             projection_store: resources.projection_store
           ) do
      {:ok, %{response: response, delivered: delivered, resources: resources}}
    end
  end

  @spec put_projection(map(), keyword()) ::
          {:ok, ChassisEvolutionProjection.Row.t()} | {:error, term()}
  def put_projection(event, opts) do
    resources = resources(opts)

    ChassisEvolutionProjection.reduce(event,
      read_store: resources.read_store,
      projection_store: resources.projection_store
    )
  end

  @spec consent_signal?(map(), keyword()) :: boolean()
  def consent_signal?(attrs, opts) do
    candidate_ref = Map.get(attrs, :candidate_ref)

    opts
    |> Keyword.get(:signals, [])
    |> Enum.any?(fn signal ->
      signal = normalize_attrs(signal)

      Map.get(signal, :type) == @signals.consent and
        (is_nil(candidate_ref) or Map.get(signal, :candidate_ref) == candidate_ref) and
        Map.get(signal, :decision, "approved") in ["approved", "approve", :approved, :approve]
    end)
  end

  @spec resources(keyword()) :: map()
  def resources(opts) do
    %{
      receipts_store: Keyword.get_lazy(opts, :receipts_store, &start_receipts_store/0),
      outbox: Keyword.get_lazy(opts, :outbox, &start_outbox/0),
      read_store: Keyword.get_lazy(opts, :read_store, &start_read_store/0),
      projection_store:
        Keyword.get(opts, :projection_store, {:file, ChassisEvolutionProjection.default_file()})
    }
  end

  defp envelope_attrs(attrs, workflow_id, step_id) do
    %{
      envelope_ref:
        Map.get(
          attrs,
          :envelope_ref,
          "env:mezzanine.chassis.evolution.#{workflow_id}.#{step_id}:#{unique()}"
        ),
      tenant_ref: Map.get(attrs, :tenant_ref, "tenant:dev"),
      installation_ref: Map.get(attrs, :installation_ref, "installation:dev"),
      actor_ref: Map.get(attrs, :actor_ref, "actor:mezzanine.workflow"),
      authority_ref: Map.get(attrs, :authority_ref, "authority:mezzanine:evolution"),
      idempotency_key: idempotency_key(workflow_id, step_id, attrs),
      trace_id: Map.get(attrs, :trace_id, "trace:mezzanine.chassis.evolution:#{unique()}"),
      correlation_id: Map.get(attrs, :correlation_id)
    }
  end

  defp request_attrs(attrs) do
    attrs
    |> normalize_attrs()
    |> Map.drop([:raw_body, :raw_diff, :raw_prompt, :raw_transcript, :provider_token])
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_attrs()

  defp normalize_attrs(attrs) when is_map(attrs) do
    attrs
    |> Enum.map(fn
      {key, value} when is_binary(key) -> {normalize_key(key), value}
      {key, value} -> {key, value}
    end)
    |> Enum.sort_by(fn {key, _value} -> inspect(key) end)
    |> Map.new()
  end

  defp normalize_attrs(_attrs), do: %{}

  defp normalize_key(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp start_receipts_store do
    {:ok, pid} = EvolutionReceiptStore.start_link(name: nil)
    pid
  end

  defp start_outbox do
    {:ok, pid} = ChassisOutbox.start_link(name: nil)
    pid
  end

  defp start_read_store do
    {:ok, pid} = ChassisEvolutionProjection.start_link(name: nil)
    pid
  end

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end

defmodule Mezzanine.Workflow.Chassis.Evolution.Result do
  @moduledoc false

  def from_step(workflow, %{response: response, delivered: delivered}, extra \\ %{}) do
    response.payload
    |> Map.new()
    |> Map.merge(extra)
    |> Map.put(:workflow, workflow)
    |> Map.put(:outbox_delivered, delivered)
  end
end

defmodule Mezzanine.Workflow.Chassis.Evolution.FailureBatchWorkflow do
  @moduledoc "Creates a Chassis evolution failure batch from bounded Mezzanine evidence refs."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, step} <-
           Engine.dispatch_step(
             :create_failure_batch,
             :failure_batch_workflow,
             :create,
             attrs,
             opts
           ) do
      {:ok, Result.from_step(:chassis_failure_batch, step)}
    end
  end
end

defmodule Mezzanine.Workflow.Chassis.Evolution.CandidatePatchWorkflow do
  @moduledoc "Starts candidate patch generation for a failure batch."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, step} <-
           Engine.dispatch_step(:evolution_start, :candidate_patch_workflow, :start, attrs, opts) do
      {:ok, Result.from_step(:chassis_candidate_patch, step)}
    end
  end
end

defmodule Mezzanine.Workflow.Chassis.Evolution.TrialReplayWorkflow do
  @moduledoc "Provisions a trial node and runs candidate replay."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, provision} <-
           Engine.dispatch_step(
             :provision_trial_node,
             :trial_replay_workflow,
             :provision,
             attrs,
             opts
           ),
         attrs = Map.put_new(attrs, :trial_ref, provision.response.payload.trial_ref),
         {:ok, replay} <-
           Engine.dispatch_step(:run_trial_replay, :trial_replay_workflow, :replay, attrs, opts) do
      {:ok,
       Result.from_step(:chassis_trial_replay, replay, %{
         trial_node_ref: provision.response.payload.trial_node_ref,
         outbox_delivered: provision.delivered + replay.delivered
       })}
    end
  end
end

defmodule Mezzanine.Workflow.Chassis.Evolution.CandidateScoringWorkflow do
  @moduledoc "Scores a candidate from trial evidence."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, step} <-
           Engine.dispatch_step(
             :score_candidate,
             :candidate_scoring_workflow,
             :score,
             attrs,
             opts
           ) do
      {:ok, Result.from_step(:chassis_candidate_scoring, step)}
    end
  end
end

defmodule Mezzanine.Workflow.Chassis.Evolution.PromotionConsentWorkflow do
  @moduledoc "Requests operator promotion consent and stops the evolution run on timeout."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, request} <-
           Engine.dispatch_step(
             :request_promotion,
             :promotion_consent_workflow,
             :request,
             attrs,
             opts
           ) do
      if timeout?(opts) and not Engine.consent_signal?(attrs, opts) do
        stop_on_timeout(attrs, opts, request)
      else
        {:ok, Result.from_step(:chassis_promotion_consent, request)}
      end
    end
  end

  defp stop_on_timeout(attrs, opts, request) do
    promotion_intent_ref = request.response.payload.promotion_intent_ref

    stop_attrs =
      attrs
      |> Map.put(
        :evolution_ref,
        Map.get(attrs, :evolution_ref, "evolution:promotion:#{promotion_intent_ref}")
      )
      |> Map.put(:reason_code, "operator_consent_timeout")

    with {:ok, stop} <-
           Engine.dispatch_step(
             :evolution_stop,
             :promotion_consent_workflow,
             :stop,
             stop_attrs,
             opts
           ),
         {:ok, _projection} <-
           Engine.put_projection(
             %{
               projection: :chassis_promotion,
               primary_ref: promotion_intent_ref,
               payload: %{
                 promotion_intent_ref: promotion_intent_ref,
                 candidate_ref: Map.get(attrs, :candidate_ref),
                 tenant_ref: Map.get(attrs, :tenant_ref, "tenant:dev"),
                 installation_ref: Map.get(attrs, :installation_ref, "installation:dev"),
                 trace_id: Map.get(attrs, :trace_id),
                 state_or_outcome: "stopped",
                 stop_reason: "operator_consent_timeout"
               }
             },
             opts
           ) do
      {:ok,
       %{
         workflow: :chassis_promotion_consent,
         status: "stopped",
         stop_reason: "operator_consent_timeout",
         promotion_intent_ref: promotion_intent_ref,
         stop_receipt_ref: stop.response.payload.receipt_ref,
         outbox_delivered: request.delivered + stop.delivered
       }}
    end
  end

  defp timeout?(opts), do: Keyword.get(opts, :consent_timeout_ms, 30_000) <= 0
end

defmodule Mezzanine.Workflow.Chassis.Evolution.PromotionApplyWorkflow do
  @moduledoc "Applies a consented candidate promotion through the Chassis bridge."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, step} <-
           Engine.dispatch_step(
             :promote_candidate,
             :promotion_apply_workflow,
             :promote,
             attrs,
             opts
           ) do
      {:ok, Result.from_step(:chassis_promotion_apply, step)}
    end
  end
end

defmodule Mezzanine.Workflow.Chassis.Evolution.SwapRollbackWorkflow do
  @moduledoc "Rolls back a promoted candidate swap through the Chassis bridge."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, step} <-
           Engine.dispatch_step(
             :rollback_candidate,
             :swap_rollback_workflow,
             :rollback,
             attrs,
             opts
           ) do
      {:ok, Result.from_step(:chassis_swap_rollback, step)}
    end
  end
end

defmodule Mezzanine.Workflow.Chassis.Evolution.ModelMaterializationWorkflow do
  @moduledoc "Requests model weight materialization by boundary ref only."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, step} <-
           Engine.dispatch_step(
             :materialize_weight,
             :model_materialization_workflow,
             :materialize,
             attrs,
             opts
           ) do
      {:ok, Result.from_step(:chassis_model_materialization, step)}
    end
  end
end

defmodule Mezzanine.Workflow.Chassis.Evolution.TensorPatchReloadWorkflow do
  @moduledoc "Requests tensor patch reload by boundary ref only."

  alias Mezzanine.Workflow.Chassis.Evolution.{Engine, Result}

  @spec dispatch(map() | keyword(), keyword()) :: {:ok, map()} | {:error, term()}
  def dispatch(attrs \\ %{}, opts \\ [])
  def dispatch(attrs, opts) when is_list(attrs), do: attrs |> Map.new() |> dispatch(opts)

  def dispatch(attrs, opts) when is_map(attrs) do
    with {:ok, step} <-
           Engine.dispatch_step(
             :reload_tensor_patch,
             :tensor_patch_reload_workflow,
             :reload,
             attrs,
             opts
           ) do
      {:ok, Result.from_step(:chassis_tensor_patch_reload, step)}
    end
  end
end

defmodule Mezzanine.Chassis.Truth.EvolutionIntentRecord do
  @moduledoc "Truth record for an evolution workflow intent."
  defstruct [:record_ref, :tenant_ref, :installation_ref, :workflow_id, :payload, :inserted_at]

  def new!(attrs),
    do: struct!(__MODULE__, Map.put_new(Map.new(attrs), :inserted_at, DateTime.utc_now()))
end

defmodule Mezzanine.Chassis.Truth.FailureBatchIntent do
  @moduledoc "Truth record for failure batch creation intent."
  defstruct [
    :record_ref,
    :tenant_ref,
    :installation_ref,
    :failure_batch_ref,
    :payload,
    :inserted_at
  ]

  def new!(attrs),
    do: struct!(__MODULE__, Map.put_new(Map.new(attrs), :inserted_at, DateTime.utc_now()))
end

defmodule Mezzanine.Chassis.Truth.CandidatePromotionIntent do
  @moduledoc "Truth record for candidate promotion intent."
  defstruct [:record_ref, :tenant_ref, :installation_ref, :candidate_ref, :payload, :inserted_at]

  def new!(attrs),
    do: struct!(__MODULE__, Map.put_new(Map.new(attrs), :inserted_at, DateTime.utc_now()))
end

defmodule Mezzanine.Chassis.Truth.OperatorConsentRecord do
  @moduledoc "Truth record for operator consent signal observation."
  defstruct [
    :record_ref,
    :tenant_ref,
    :installation_ref,
    :candidate_ref,
    :decision,
    :payload,
    :inserted_at
  ]

  def new!(attrs),
    do: struct!(__MODULE__, Map.put_new(Map.new(attrs), :inserted_at, DateTime.utc_now()))
end

defmodule Mezzanine.Chassis.Truth.ModelMaterializationIntent do
  @moduledoc "Truth record for model materialization intent."
  defstruct [
    :record_ref,
    :tenant_ref,
    :installation_ref,
    :model_ref,
    :target_host_ref,
    :payload,
    :inserted_at
  ]

  def new!(attrs),
    do: struct!(__MODULE__, Map.put_new(Map.new(attrs), :inserted_at, DateTime.utc_now()))
end

defmodule Mezzanine.Chassis.Truth.TensorReloadIntent do
  @moduledoc "Truth record for tensor reload intent."
  defstruct [
    :record_ref,
    :tenant_ref,
    :installation_ref,
    :patch_ref,
    :target_runtime_ref,
    :payload,
    :inserted_at
  ]

  def new!(attrs),
    do: struct!(__MODULE__, Map.put_new(Map.new(attrs), :inserted_at, DateTime.utc_now()))
end

defmodule Mezzanine.Read.ChassisDeploymentProjection do
  @moduledoc "Chassis deployment read projection."

  alias Chassis.Projection.DeploymentStatus
  alias Chassis.Projection.Store

  @fields Map.keys(Map.from_struct(%DeploymentStatus{}))

  @spec default_file() :: String.t()
  def default_file do
    Path.join(System.tmp_dir!(), "mezzanine_chassis_deployment_projection.term")
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: Store.Memory.start_link(opts)

  @spec reduce(map(), keyword()) :: {:ok, DeploymentStatus.t()} | {:error, term()}
  def reduce(%{kind: :chassis_deployment, payload: payload}, opts) do
    with {:ok, projection} <- projection_from_payload(payload),
         :ok <- put_all(projection, opts) do
      {:ok, projection}
    end
  end

  def reduce(%{kind: kind}, _opts), do: {:error, {:unsupported_projection_event, kind}}
  def reduce(_event, _opts), do: {:error, :invalid_projection_event}

  @spec latest(GenServer.server() | {:file, String.t()}, keyword()) ::
          {:ok, DeploymentStatus.t()} | {:error, term()}
  def latest({:file, path}, query) do
    path
    |> read_file()
    |> latest_from_list(query)
  end

  def latest(server, query), do: Store.Memory.latest(server, query)

  @spec list(GenServer.server() | {:file, String.t()}) :: [DeploymentStatus.t()]
  def list({:file, path}), do: read_file(path)
  def list(server), do: Store.Memory.list(server)

  defp projection_from_payload(%DeploymentStatus{} = projection), do: {:ok, projection}

  defp projection_from_payload(payload) when is_map(payload) do
    attrs =
      payload
      |> Enum.reduce(%{}, fn {key, value}, acc ->
        case normalize_key(key) do
          key when key in @fields -> Map.put(acc, key, value)
          _unknown -> acc
        end
      end)

    projection = struct(DeploymentStatus, attrs)

    if is_binary(projection.receipt_ref) and projection.receipt_ref != "" do
      {:ok, projection}
    else
      {:error, :receipt_ref_required}
    end
  end

  defp projection_from_payload(_payload), do: {:error, :invalid_projection_payload}

  defp put_all(projection, opts) do
    opts
    |> stores()
    |> Enum.reduce_while(:ok, fn store, :ok ->
      case put(store, projection) do
        {:ok, _projection} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp stores(opts) do
    [
      Keyword.get(opts, :read_store),
      Keyword.get(opts, :store),
      Keyword.get(opts, :projection_store)
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp put({:file, path}, projection), do: put_file(path, projection)
  defp put(server, projection), do: Store.Memory.put(server, projection)

  defp put_file(path, projection) do
    if source_repo_path?(path) do
      {:error, {:runtime_state_path_in_source_repo, Path.expand(path)}}
    else
      projections =
        path
        |> read_file()
        |> upsert_projection(projection)

      path = Path.expand(path)
      File.mkdir_p!(Path.dirname(path))
      temp = path <> ".tmp." <> unique()
      File.write!(temp, :erlang.term_to_binary(Enum.map(projections, &file_payload/1)))
      File.rename(temp, path)
      {:ok, projection}
    end
  end

  defp read_file(path) do
    path
    |> Path.expand()
    |> File.read()
    |> decode_projection_file()
  rescue
    ArgumentError -> []
  end

  defp decode_projection_file({:ok, bytes}) do
    bytes
    |> :erlang.binary_to_term([:safe])
    |> Enum.reduce([], fn payload, acc ->
      case projection_from_payload(payload) do
        {:ok, projection} -> [projection | acc]
        {:error, _reason} -> acc
      end
    end)
    |> Enum.reverse()
  end

  defp decode_projection_file({:error, _reason}), do: []

  defp upsert_projection(projections, projection) do
    projections
    |> Enum.reject(&(&1.receipt_ref == projection.receipt_ref))
    |> Kernel.++([projection])
  end

  defp latest_from_list(projections, query) do
    projections
    |> Enum.reverse()
    |> Enum.find(&matches_query?(&1, query))
    |> case do
      nil -> {:error, :not_found}
      projection -> {:ok, projection}
    end
  end

  defp matches_query?(%DeploymentStatus{} = projection, query) do
    Enum.all?(query, fn {key, expected} -> Map.get(projection, key) == expected end)
  end

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    Enum.find(@fields, &(Atom.to_string(&1) == key))
  end

  defp normalize_key(_key), do: nil

  defp file_payload(%DeploymentStatus{} = projection) do
    projection
    |> Map.from_struct()
    |> Map.new(fn {key, value} -> {Atom.to_string(key), file_value(value)} end)
  end

  defp file_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp file_value(nil), do: nil
  defp file_value(value) when is_atom(value), do: Atom.to_string(value)

  defp file_value(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {to_string(key), file_value(nested)} end)
  end

  defp file_value(value) when is_list(value), do: Enum.map(value, &file_value/1)
  defp file_value(value), do: value

  defp source_repo_path?(path) do
    repo_root = Path.expand(File.cwd!())
    String.starts_with?(Path.expand(path), repo_root)
  end

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end

defmodule Mezzanine.Read.ChassisEvolutionProjection do
  @moduledoc "Chassis evolution read projection."

  use GenServer

  @supported_projections MapSet.new([
                           :chassis_evolution,
                           :chassis_candidate,
                           :chassis_trial,
                           :chassis_score_matrix,
                           :chassis_promotion,
                           :chassis_swap,
                           :chassis_model_materialization,
                           :chassis_tensor_reload
                         ])

  defmodule Row do
    @moduledoc "Operator-safe Chassis evolution read projection row."

    defstruct [
      :projection,
      :primary_ref,
      :tenant_ref,
      :installation_ref,
      :state_or_outcome,
      :summary,
      :authority_ref,
      :operator_consent_ref,
      :trace_id,
      :last_updated_at
    ]

    @type t :: %__MODULE__{}
  end

  @spec default_file() :: String.t()
  def default_file do
    Path.join(System.tmp_dir!(), "mezzanine_chassis_evolution_projection.term")
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)

    case name do
      nil -> GenServer.start_link(__MODULE__, opts, [])
      _name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  @spec reduce(map(), keyword()) :: {:ok, Row.t()} | {:error, term()}
  def reduce(event, opts \\ [])

  def reduce(event, opts) when is_map(event) do
    with {:ok, row} <- row_from_event(event),
         :ok <- put_all(row, opts) do
      {:ok, row}
    end
  end

  def reduce(_event, _opts), do: {:error, :invalid_projection_event}

  @spec latest(GenServer.server() | {:file, String.t()}, keyword()) ::
          {:ok, Row.t()} | {:error, :not_found}
  def latest({:file, path}, query), do: path |> read_file() |> latest_from_list(query)
  def latest(server, query), do: GenServer.call(server, {:latest, query})

  @spec list(GenServer.server() | {:file, String.t()}) :: [Row.t()]
  def list({:file, path}), do: read_file(path)
  def list(server), do: GenServer.call(server, :list)

  @spec put(GenServer.server() | {:file, String.t()}, Row.t()) ::
          {:ok, Row.t()} | {:error, term()}
  def put({:file, path}, %Row{} = row), do: put_file(path, row)
  def put(server, %Row{} = row), do: GenServer.call(server, {:put, row})

  @spec last() :: {:ok, Row.t()} | {:error, :not_found}
  def last, do: latest({:file, default_file()}, [])

  @impl true
  def init(_opts), do: {:ok, %{by_key: %{}, order: []}}

  @impl true
  def handle_call({:put, %Row{} = row}, _from, state) do
    key = {row.projection, row.primary_ref}
    exists? = Map.has_key?(state.by_key, key)

    state = %{
      state
      | by_key: Map.put(state.by_key, key, row),
        order: if(exists?, do: state.order, else: [key | state.order])
    }

    {:reply, {:ok, row}, state}
  end

  def handle_call({:latest, query}, _from, state),
    do: {:reply, latest_from_list(rows(state), query), state}

  def handle_call(:list, _from, state), do: {:reply, rows(state), state}

  defp row_from_event(event) do
    event = normalize_event(event)
    projection = normalize_projection(value(event, :projection) || value(event, :kind))
    payload = event |> value(:payload, %{}) |> safe_payload()
    primary_ref = value(event, :primary_ref) || primary_ref(payload)

    with :ok <- supported_projection(projection, event),
         :ok <- require_primary_ref(primary_ref) do
      {:ok, build_row(event, payload, projection, primary_ref)}
    end
  end

  defp supported_projection(projection, _event)
       when projection in [
              :chassis_evolution,
              :chassis_candidate,
              :chassis_trial,
              :chassis_score_matrix,
              :chassis_promotion,
              :chassis_swap,
              :chassis_model_materialization,
              :chassis_tensor_reload
            ],
       do: :ok

  defp supported_projection(projection, event),
    do: {:error, {:unsupported_projection_event, projection || value(event, :kind)}}

  defp require_primary_ref(primary_ref) when is_binary(primary_ref) and primary_ref != "", do: :ok
  defp require_primary_ref(_primary_ref), do: {:error, :primary_ref_required}

  defp build_row(event, payload, projection, primary_ref) do
    %Row{
      projection: projection,
      primary_ref: primary_ref,
      tenant_ref: value(event, :tenant_ref) || value(payload, :tenant_ref),
      installation_ref: value(event, :installation_ref) || value(payload, :installation_ref),
      state_or_outcome: state_or_outcome(payload),
      summary: payload,
      authority_ref: value(payload, :authority_ref),
      operator_consent_ref: value(payload, :operator_consent_ref),
      trace_id: value(event, :trace_id) || value(payload, :trace_id),
      last_updated_at: DateTime.utc_now()
    }
  end

  defp normalize_event(%_struct{} = event), do: Map.from_struct(event)
  defp normalize_event(event), do: Map.new(event)

  defp normalize_projection(value) when is_atom(value), do: value

  defp normalize_projection(value) when is_binary(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> nil
  end

  defp normalize_projection(_value), do: nil

  defp primary_ref(payload) do
    Enum.find_value(
      [
        :failure_batch_ref,
        :candidate_ref,
        :trial_ref,
        :trial_run_ref,
        :score_matrix_ref,
        :promotion_ref,
        :promotion_intent_ref,
        :swap_ref,
        :rollback_ref,
        :materialization_record_ref,
        :tensor_reload_record_ref,
        :tensor_rollback_record_ref,
        :receipt_ref
      ],
      fn key ->
        case value(payload, key) do
          ref when is_binary(ref) and ref != "" -> ref
          _missing -> nil
        end
      end
    )
  end

  defp state_or_outcome(payload) do
    value(payload, :state_or_outcome) ||
      value(payload, :terminal_state) ||
      value(payload, :outcome) ||
      value(payload, :verdict) ||
      value(payload, :regression_gate) ||
      value(payload, :status) ||
      "observed"
  end

  defp put_all(row, opts) do
    opts
    |> stores()
    |> Enum.reduce_while(:ok, fn store, :ok ->
      case put(store, row) do
        {:ok, _row} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp stores(opts) do
    [
      Keyword.get(opts, :read_store),
      Keyword.get(opts, :store),
      Keyword.get(opts, :projection_store)
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp rows(state), do: Enum.map(state.order, &Map.fetch!(state.by_key, &1))

  defp put_file(path, %Row{} = row) do
    if source_repo_path?(path) do
      {:error, {:runtime_state_path_in_source_repo, Path.expand(path)}}
    else
      rows =
        path
        |> read_file()
        |> upsert_row(row)

      path = Path.expand(path)
      File.mkdir_p!(Path.dirname(path))
      temp = path <> ".tmp." <> unique()
      File.write!(temp, :erlang.term_to_binary(Enum.map(rows, &file_payload/1)))
      File.rename(temp, path)
      {:ok, row}
    end
  end

  defp read_file(path) do
    path
    |> Path.expand()
    |> File.read()
    |> decode_row_file()
  rescue
    ArgumentError -> []
  end

  defp decode_row_file({:ok, bytes}) do
    bytes
    |> :erlang.binary_to_term([:safe])
    |> Enum.reduce([], fn payload, acc ->
      case row_from_file(payload) do
        {:ok, row} -> [row | acc]
        {:error, _reason} -> acc
      end
    end)
    |> Enum.reverse()
  end

  defp decode_row_file({:error, _reason}), do: []

  defp row_from_file(payload) when is_map(payload) do
    attrs = normalize_event(payload)
    projection = normalize_projection(value(attrs, :projection))
    primary_ref = value(attrs, :primary_ref)

    if MapSet.member?(@supported_projections, projection) and is_binary(primary_ref) do
      {:ok,
       %Row{
         projection: projection,
         primary_ref: primary_ref,
         tenant_ref: value(attrs, :tenant_ref),
         installation_ref: value(attrs, :installation_ref),
         state_or_outcome: value(attrs, :state_or_outcome),
         summary: value(attrs, :summary, %{}) |> safe_payload(),
         authority_ref: value(attrs, :authority_ref),
         operator_consent_ref: value(attrs, :operator_consent_ref),
         trace_id: value(attrs, :trace_id),
         last_updated_at: parse_datetime(value(attrs, :last_updated_at))
       }}
    else
      {:error, :invalid_projection_file_row}
    end
  end

  defp row_from_file(_payload), do: {:error, :invalid_projection_file_row}

  defp upsert_row(rows, row) do
    rows
    |> Enum.reject(&(&1.projection == row.projection and &1.primary_ref == row.primary_ref))
    |> Kernel.++([row])
  end

  defp latest_from_list(rows, query) do
    rows
    |> Enum.find(&matches_query?(&1, query))
    |> case do
      nil -> {:error, :not_found}
      row -> {:ok, row}
    end
  end

  defp matches_query?(%Row{} = row, query) do
    Enum.all?(query, fn {key, expected} -> Map.get(row, key) == expected end)
  end

  defp safe_payload(payload) when is_map(payload) do
    payload
    |> normalize_event()
    |> Enum.reject(fn {key, _value} -> sensitive_key?(key) end)
    |> Map.new(fn {key, value} -> {normalize_key(key), safe_value(value)} end)
  end

  defp safe_payload(_payload), do: %{}

  defp safe_value(%DateTime{} = value), do: value
  defp safe_value(value) when is_map(value), do: safe_payload(value)
  defp safe_value(value) when is_list(value), do: Enum.map(value, &safe_value/1)
  defp safe_value(value), do: value

  defp file_payload(%Row{} = row) do
    row
    |> Map.from_struct()
    |> Map.new(fn {key, value} -> {Atom.to_string(key), file_value(value)} end)
  end

  defp file_value(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp file_value(value) when is_atom(value), do: Atom.to_string(value)

  defp file_value(value) when is_map(value),
    do: Map.new(value, fn {key, nested} -> {to_string(key), file_value(nested)} end)

  defp file_value(value) when is_list(value), do: Enum.map(value, &file_value/1)
  defp file_value(value), do: value

  defp parse_datetime(%DateTime{} = value), do: value

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _error -> nil
    end
  end

  defp parse_datetime(_value), do: nil

  defp sensitive_key?(key) when is_atom(key),
    do: key in [:raw_body, :raw_diff, :raw_prompt, :raw_transcript, :provider_token]

  defp sensitive_key?(key) when is_binary(key) do
    key
    |> String.to_existing_atom()
    |> sensitive_key?()
  rescue
    ArgumentError -> false
  end

  defp normalize_key(key) when is_binary(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp normalize_key(key), do: key

  defp value(map, key, default \\ nil) when is_map(map) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp source_repo_path?(path) do
    repo_root = Path.expand(File.cwd!())
    String.starts_with?(Path.expand(path), repo_root)
  end

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end

defmodule Mezzanine.Read.ChassisCandidateProjection do
  @moduledoc "Candidate patch read projection facade."

  alias Mezzanine.Read.ChassisEvolutionProjection, as: Projection

  def latest(store, query \\ []),
    do:
      Projection.latest(
        store,
        Keyword.put(query, :projection, :chassis_candidate)
      )

  def list(store), do: Projection.list(store)
end

defmodule Mezzanine.Read.ChassisTrialProjection do
  @moduledoc "Trial replay read projection facade."

  alias Mezzanine.Read.ChassisEvolutionProjection, as: Projection

  def latest(store, query \\ []),
    do:
      Projection.latest(
        store,
        Keyword.put(query, :projection, :chassis_trial)
      )

  def list(store), do: Projection.list(store)
end

defmodule Mezzanine.Read.ChassisScoreMatrixProjection do
  @moduledoc "Candidate score matrix read projection facade."

  alias Mezzanine.Read.ChassisEvolutionProjection, as: Projection

  def latest(store, query \\ []),
    do:
      Projection.latest(
        store,
        Keyword.put(query, :projection, :chassis_score_matrix)
      )

  def list(store), do: Projection.list(store)
end

defmodule Mezzanine.Read.ChassisPromotionProjection do
  @moduledoc "Promotion consent and apply read projection facade."

  alias Mezzanine.Read.ChassisEvolutionProjection, as: Projection

  def latest(store, query \\ []),
    do:
      Projection.latest(
        store,
        Keyword.put(query, :projection, :chassis_promotion)
      )

  def list(store), do: Projection.list(store)
end

defmodule Mezzanine.Read.ChassisSwapProjection do
  @moduledoc "Swap and rollback read projection facade."

  alias Mezzanine.Read.ChassisEvolutionProjection, as: Projection

  def latest(store, query \\ []),
    do:
      Projection.latest(
        store,
        Keyword.put(query, :projection, :chassis_swap)
      )

  def list(store), do: Projection.list(store)
end

defmodule Mezzanine.Read.ChassisModelMaterializationProjection do
  @moduledoc "Model materialization read projection facade."

  alias Mezzanine.Read.ChassisEvolutionProjection, as: Projection

  def latest(store, query \\ []),
    do:
      Projection.latest(
        store,
        Keyword.put(query, :projection, :chassis_model_materialization)
      )

  def list(store), do: Projection.list(store)
end

defmodule Mezzanine.Read.ChassisTensorReloadProjection do
  @moduledoc "Tensor reload read projection facade."

  alias Mezzanine.Read.ChassisEvolutionProjection, as: Projection

  def latest(store, query \\ []),
    do:
      Projection.latest(
        store,
        Keyword.put(query, :projection, :chassis_tensor_reload)
      )

  def list(store), do: Projection.list(store)
end

defmodule Mezzanine.Outbox.ChassisDrainWorker do
  @moduledoc "Drains Chassis deployment outbox events into Mezzanine read projections."

  alias Chassis.Mezzanine.Bridge.Outbox, as: ChassisOutbox
  alias Mezzanine.Read.ChassisDeploymentProjection
  alias Mezzanine.Read.ChassisEvolutionProjection

  @spec drain(GenServer.server(), keyword()) :: {:ok, map()} | {:error, term()}
  def drain(outbox, opts \\ []) do
    ChassisOutbox.drain(outbox,
      publisher: fn event ->
        case Map.get(event, :kind) do
          :chassis_deployment -> ChassisDeploymentProjection.reduce(event, opts)
          "chassis_deployment" -> ChassisDeploymentProjection.reduce(event, opts)
          _other -> ChassisEvolutionProjection.reduce(event, opts)
        end
      end
    )
  end
end
