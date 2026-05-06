defmodule Mezzanine.AIRun.RunClass do
  @moduledoc "Allowed AI run classes."

  @classes [
    :inference_call,
    :self_hosted_endpoint_session,
    :optimization_run,
    :optimization_candidate_eval,
    :coordination_run,
    :router_decision,
    :role_injection,
    :tool_operation,
    :eval_run,
    :replay_run,
    :prompt_promotion,
    :skill_invocation,
    :closed_loop_adaptation
  ]

  @type t ::
          :inference_call
          | :self_hosted_endpoint_session
          | :optimization_run
          | :optimization_candidate_eval
          | :coordination_run
          | :router_decision
          | :role_injection
          | :tool_operation
          | :eval_run
          | :replay_run
          | :prompt_promotion
          | :skill_invocation
          | :closed_loop_adaptation

  @spec all() :: [t()]
  def all, do: @classes

  @spec valid?(term()) :: boolean()
  def valid?(class), do: class in @classes
end
