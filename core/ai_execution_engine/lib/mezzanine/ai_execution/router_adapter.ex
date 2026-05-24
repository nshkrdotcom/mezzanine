defmodule Mezzanine.AIExecution.RouterAdapter do
  @moduledoc """
  Behaviour implemented by TRINITY-compatible route adapters.
  """

  alias OuterBrain.ContextABI.Failure

  @type route_request :: %{
          required(:tenant_ref) => String.t(),
          required(:workflow_ref) => String.t(),
          required(:context_packet_ref) => String.t(),
          required(:packet_hash) => String.t(),
          required(:authority_ref) => String.t(),
          required(:route_policy_ref) => String.t(),
          required(:model_class_allowlist) => [String.t()],
          required(:trace_ref) => String.t()
        }

  @type route_decision :: %{
          required(:route_decision_ref) => String.t(),
          required(:selected_route_kind) => atom(),
          required(:selected_model_profile_ref) => String.t(),
          required(:route_policy_ref) => String.t(),
          required(:reason_codes) => [String.t()],
          required(:trace_ref) => String.t()
        }

  @callback route(route_request(), keyword()) :: {:ok, route_decision()} | {:error, Failure.t()}
end
