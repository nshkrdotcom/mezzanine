defmodule Mezzanine.AIExecution do
  @moduledoc """
  Public facade for Mezzanine-owned generalized AI execution contracts.
  """

  alias Mezzanine.AIExecution.{
    FixtureOptimizerAdapter,
    FixtureRouterAdapter,
    OptimizerAdapter,
    RenderResult,
    RouterAdapter,
    RuntimeDeps
  }

  alias OuterBrain.ContextABI.{ContextPacket, Failure}

  @manifest %{
    package: :mezzanine_ai_execution_engine,
    layer: :core,
    status: :nshkr_fugu_phase_5_ai_execution_contracts,
    owns: [
      :router_adapter_contract,
      :optimizer_adapter_contract,
      :render_result_handoff,
      :model_invocation_request_assembly,
      :explicit_runtime_dependencies
    ],
    internal_dependencies: [:mezzanine_context_packet_engine],
    external_dependencies: [
      :outer_brain_context_abi,
      :outer_brain_prompting,
      :ground_plane_contracts
    ]
  }

  @spec manifest() :: map()
  def manifest, do: @manifest

  @spec router_adapter_module() :: module()
  def router_adapter_module, do: RouterAdapter

  @spec optimizer_adapter_module() :: module()
  def optimizer_adapter_module, do: OptimizerAdapter

  @spec default_router_adapter_module() :: module()
  def default_router_adapter_module, do: FixtureRouterAdapter

  @spec default_optimizer_adapter_module() :: module()
  def default_optimizer_adapter_module, do: FixtureOptimizerAdapter

  @spec runtime_deps_module() :: module()
  def runtime_deps_module, do: RuntimeDeps

  @spec render_result_module() :: module()
  def render_result_module, do: RenderResult

  @spec route(RouterAdapter.route_request(), RuntimeDeps.t() | map() | keyword(), keyword()) ::
          {:ok, RouterAdapter.route_decision()} | {:error, Failure.t()}
  def route(route_request, runtime_deps \\ %RuntimeDeps{}, opts \\ []) do
    deps = RuntimeDeps.new!(runtime_deps)
    adapter = deps.router_adapter || FixtureRouterAdapter
    adapter.route(route_request, opts)
  end

  @spec propose(
          OptimizerAdapter.optimization_request(),
          RuntimeDeps.t() | map() | keyword(),
          keyword()
        ) ::
          {:ok, [OptimizerAdapter.candidate_receipt()]} | {:error, Failure.t()}
  def propose(optimization_request, runtime_deps \\ %RuntimeDeps{}, opts \\ []) do
    deps = RuntimeDeps.new!(runtime_deps)
    adapter = deps.optimizer_adapter || FixtureOptimizerAdapter
    adapter.propose(optimization_request, opts)
  end

  @spec render_context(
          ContextPacket.t(),
          RouterAdapter.route_decision(),
          RuntimeDeps.t() | map() | keyword(),
          keyword()
        ) :: {:ok, RenderResult.t()} | {:error, Failure.t()}
  def render_context(%ContextPacket{} = packet, route_decision, runtime_deps, opts \\ []) do
    deps = RuntimeDeps.new!(runtime_deps)
    renderer = deps.renderer || OuterBrain.Prompting.ContextRenderer.Fixture
    profile = render_profile(route_decision, opts)

    with {:ok, rendered} <- renderer.render(packet, profile, opts) do
      RenderResult.from_rendered(packet, route_decision, rendered, opts)
    end
  end

  @spec invocation_request(RenderResult.t(), RouterAdapter.route_decision(), keyword()) ::
          {:ok, map()} | {:error, Failure.t()}
  def invocation_request(%RenderResult{} = render_result, route_decision, opts \\ []) do
    with {:ok, selected_model_profile_ref} <-
           required(route_decision, :selected_model_profile_ref),
         {:ok, idempotency_key} <-
           required(opts, :idempotency_key, "idem://#{render_result.payload_hash}") do
      {:ok,
       %{
         tenant_ref: render_result.tenant_ref,
         workflow_ref: render_result.workflow_ref,
         context_packet_ref: render_result.context_packet_ref,
         route_decision_ref: render_result.route_decision_ref,
         prompt_artifact_ref: render_result.prompt_artifact_ref,
         provider_payload_ref: render_result.provider_payload_ref,
         model_profile_ref: selected_model_profile_ref,
         credential_lease_ref: Keyword.get(opts, :credential_lease_ref),
         idempotency_key: idempotency_key,
         trace_ref: render_result.trace_ref
       }}
    end
  end

  defp render_profile(route_decision, opts) do
    %{
      provider_family: Keyword.get(opts, :provider_family, provider_family(route_decision)),
      model_class: Keyword.get(opts, :model_class, selected_model_profile(route_decision)),
      payload_mode: Keyword.get(opts, :payload_mode, :ref_only)
    }
  end

  defp provider_family(route_decision),
    do:
      Map.get(route_decision, :provider_family) ||
        Map.get(route_decision, "provider_family", "fixture")

  defp selected_model_profile(route_decision) do
    Map.get(route_decision, :selected_model_profile_ref) ||
      Map.get(route_decision, "selected_model_profile_ref") ||
      "model-profile://fixture"
  end

  defp required(attrs, field, default \\ nil)

  defp required(attrs, field, default) when is_list(attrs) do
    case Keyword.get(attrs, field, default) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> failure("mezzanine.ai_execution.missing_invocation_ref.v1", field)
    end
  end

  defp required(attrs, field, default) when is_map(attrs) do
    case Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field), default) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> failure("mezzanine.ai_execution.missing_invocation_ref.v1", field)
    end
  end

  defp failure(reason_code, field) do
    {:ok, failure} =
      Failure.new(%{
        owner: :mezzanine,
        reason_code: reason_code,
        safe_message: "AI execution invocation request is missing a required ref",
        evidence_refs: ["field://#{Atom.to_string(field)}"]
      })

    {:error, failure}
  end
end
