defmodule Mezzanine.AIExecution.FixtureRouterAdapter do
  @moduledoc """
  Deterministic router adapter for local proof and package tests.
  """

  @behaviour Mezzanine.AIExecution.RouterAdapter

  alias GroundPlane.Boundary.Codec
  alias OuterBrain.ContextABI.Failure

  @impl true
  def route(route_request, opts \\ [])

  def route(route_request, _opts) when is_map(route_request) do
    with :ok <- reject_raw(route_request),
         {:ok, tenant_ref} <- required(route_request, :tenant_ref),
         {:ok, workflow_ref} <- required(route_request, :workflow_ref),
         {:ok, context_packet_ref} <- required(route_request, :context_packet_ref),
         {:ok, packet_hash} <- required(route_request, :packet_hash),
         {:ok, authority_ref} <- required(route_request, :authority_ref),
         {:ok, route_policy_ref} <- required(route_request, :route_policy_ref),
         {:ok, trace_ref} <- required(route_request, :trace_ref),
         {:ok, model_classes} <- model_classes(route_request) do
      selected_model_profile_ref = List.first(model_classes)

      {:ok,
       %{
         route_decision_ref:
           route_decision_ref(%{
             tenant_ref: tenant_ref,
             workflow_ref: workflow_ref,
             context_packet_ref: context_packet_ref,
             packet_hash: packet_hash,
             authority_ref: authority_ref,
             route_policy_ref: route_policy_ref,
             selected_model_profile_ref: selected_model_profile_ref,
             trace_ref: trace_ref
           }),
         selected_route_kind: :fixture,
         selected_model_profile_ref: selected_model_profile_ref,
         route_policy_ref: route_policy_ref,
         reason_codes: ["mezzanine.ai_execution.fixture_route.v1"],
         trace_ref: trace_ref,
         provider_family: "fixture"
       }}
    end
  end

  def route(_route_request, _opts),
    do:
      failure("mezzanine.ai_execution.invalid_route_request.v1",
        safe_message: "route request is invalid"
      )

  defp model_classes(route_request) do
    case Map.get(route_request, :model_class_allowlist) ||
           Map.get(route_request, "model_class_allowlist", []) do
      [first | _rest] = values when is_binary(first) ->
        {:ok, values}

      _other ->
        failure("mezzanine.ai_execution.missing_route_ref.v1",
          safe_message: "route request is missing a model class allowlist",
          evidence_refs: ["field://model_class_allowlist"]
        )
    end
  end

  defp route_decision_ref(payload) do
    payload
    |> Codec.digest()
    |> String.replace_prefix("sha256:", "route-decision://")
  end

  defp required(attrs, field) do
    case Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field)) do
      value when is_binary(value) and value != "" ->
        {:ok, value}

      _other ->
        failure("mezzanine.ai_execution.missing_route_ref.v1",
          safe_message: "route request is missing a required ref",
          evidence_refs: ["field://#{Atom.to_string(field)}"]
        )
    end
  end

  defp reject_raw(attrs) do
    case Enum.find(Map.keys(attrs), &String.starts_with?(to_string(&1), "raw_")) do
      nil ->
        :ok

      key ->
        failure("mezzanine.ai_execution.raw_payload_rejected.v1",
          safe_message: "route request cannot carry raw payloads",
          evidence_refs: ["field://#{to_string(key)}"]
        )
    end
  end

  defp failure(reason_code, opts) do
    {:ok, failure} =
      Failure.new(%{
        owner: :mezzanine,
        reason_code: reason_code,
        safe_message: Keyword.fetch!(opts, :safe_message),
        evidence_refs: Keyword.get(opts, :evidence_refs, [])
      })

    {:error, failure}
  end
end
