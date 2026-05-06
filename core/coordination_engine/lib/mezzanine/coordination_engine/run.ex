defmodule Mezzanine.CoordinationEngine.Run do
  @moduledoc """
  Coordination lifecycle state with ref-only receipts.
  """

  alias Mezzanine.CoordinationEngine.RunSpec

  @enforce_keys [:spec, :ai_run_envelope, :state, :state_history]
  defstruct [
    :spec,
    :ai_run_envelope,
    :state,
    :provider_pool_ref,
    :router_decision_ref,
    :selected_role_ref,
    :current_turn_ref,
    :verifier_policy,
    :verifier_result_ref,
    :handoff_ref,
    :termination_ref,
    :cancellation_ref,
    :failure_ref,
    :replacement_ref,
    state_history: [],
    trace_refs: [],
    replay_refs: []
  ]

  @type t :: %__MODULE__{}

  @spec new(RunSpec.t()) :: {:ok, t()} | {:error, term()}
  def new(%RunSpec{} = spec) do
    with {:ok, envelope} <- Mezzanine.AIRun.new(RunSpec.to_ai_run_attrs(spec)) do
      {:ok,
       %__MODULE__{
         spec: spec,
         ai_run_envelope: envelope,
         state: :created,
         provider_pool_ref: spec.provider_pool_ref,
         state_history: [:created],
         trace_refs: spec.trace_ref_set,
         replay_refs: [spec.replay_ref]
       }}
    end
  end
end
