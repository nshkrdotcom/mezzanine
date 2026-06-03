defmodule Mezzanine.Workflow.ChassisDeploymentWorkflow do
  @moduledoc "Mezzanine workflow facade for Chassis deployment materialization."

  alias Chassis.Boundary
  alias Chassis.Mezzanine.Bridge
  alias Chassis.Receipts.Store
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
    {:ok, pid} = Chassis.AppRegistry.start_link(name: nil)
    pid
  end

  defp start_fence_store do
    {:ok, pid} = Chassis.StackManager.FenceStore.start_link(name: nil)
    pid
  end

  defp start_outbox do
    {:ok, pid} = Chassis.Mezzanine.Bridge.Outbox.start_link(name: nil)
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

  alias Chassis.Boundary
  alias Chassis.Receipts.{DeploymentRecord, Store}

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
           Chassis.Mezzanine.Bridge.dispatch(
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
           Chassis.AppRegistry.Entry.new(%{
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
         {:ok, _entry} <- Chassis.AppRegistry.register(resources.registry, entry) do
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
    {:ok, pid} = Chassis.AppRegistry.start_link(name: nil)
    pid
  end

  defp start_checkpoint_store do
    {:ok, pid} = Chassis.StackManager.CheckpointStore.start_link(name: nil)
    pid
  end

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end

for workflow <- [
      FailureBatchWorkflow,
      CandidatePatchWorkflow,
      TrialReplayWorkflow,
      CandidateScoringWorkflow,
      PromotionConsentWorkflow,
      PromotionApplyWorkflow,
      SwapRollbackWorkflow,
      ModelMaterializationWorkflow,
      TensorPatchReloadWorkflow
    ] do
  defmodule Module.concat(Mezzanine.Workflow.Chassis.Evolution, workflow) do
    @moduledoc "Chassis Evolution workflow facade."
    def dispatch(_attrs \\ %{}), do: {:error, {:not_implemented, __MODULE__}}
  end
end

for record <- [
      EvolutionIntentRecord,
      FailureBatchIntent,
      CandidatePromotionIntent,
      OperatorConsentRecord,
      ModelMaterializationIntent,
      TensorReloadIntent
    ] do
  defmodule Module.concat(Mezzanine.Chassis.Truth, record) do
    @moduledoc "Chassis Truth record."
    defstruct [:record_ref, :tenant_ref, :payload]
  end
end

defmodule Mezzanine.Read.ChassisDeploymentProjection do
  @moduledoc "Chassis deployment read projection."

  alias Chassis.Projection.DeploymentStatus
  alias Chassis.Projection.Store

  @fields Map.keys(Map.from_struct(%DeploymentStatus{}))

  @spec default_file() :: String.t()
  def default_file do
    System.get_env("MEZZANINE_CHASSIS_PROJECTION_FILE") ||
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
    path = Path.expand(path)

    if File.exists?(path) do
      path
      |> File.read!()
      |> :erlang.binary_to_term([:safe])
      |> Enum.reduce([], fn payload, acc ->
        case projection_from_payload(payload) do
          {:ok, projection} -> [projection | acc]
          {:error, _reason} -> acc
        end
      end)
      |> Enum.reverse()
    else
      []
    end
  rescue
    ArgumentError -> []
  end

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
    repo_root = Path.expand(System.get_env("MEZZANINE_CHASSIS_SOURCE_ROOT") || File.cwd!())
    String.starts_with?(Path.expand(path), repo_root)
  end

  defp unique, do: System.unique_integer([:positive]) |> Integer.to_string()
end

defmodule Mezzanine.Read.ChassisEvolutionProjection do
  @moduledoc "Chassis evolution read projection."
  def last, do: {:error, {:not_implemented, __MODULE__}}
end

defmodule Mezzanine.Outbox.ChassisDrainWorker do
  @moduledoc "Drains Chassis deployment outbox events into Mezzanine read projections."

  alias Mezzanine.Read.ChassisDeploymentProjection

  @spec drain(GenServer.server(), keyword()) :: {:ok, map()} | {:error, term()}
  def drain(outbox, opts \\ []) do
    Chassis.Mezzanine.Bridge.Outbox.drain(outbox,
      publisher: fn event -> ChassisDeploymentProjection.reduce(event, opts) end
    )
  end
end
