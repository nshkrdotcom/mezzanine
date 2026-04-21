defmodule MezzanineExecutionEngine do
  @moduledoc """
  Neutral execution-ledger and Temporal workflow handoff entrypoint for the Mezzanine
  rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.Execution,
      Mezzanine.Execution.ExecutionRecord,
      Mezzanine.Execution.LifecycleContinuation,
      Mezzanine.Execution.OwnerDirectedCompensation,
      Mezzanine.Execution.BoundedContextRepairRouting,
      Mezzanine.Execution.CompensationEvidence,
      Mezzanine.Execution.OperatorActionClassification,
      Mezzanine.LowerGateway,
      Mezzanine.Installations,
      Mezzanine.WorkControl,
      Mezzanine.WorkQueries,
      Mezzanine.OperatorActions,
      Mezzanine.Reviews,
      Mezzanine.Execution.RuntimeStack
    ]
  end
end
