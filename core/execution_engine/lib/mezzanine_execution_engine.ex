defmodule MezzanineExecutionEngine do
  @moduledoc """
  Neutral execution-ledger and dispatch-worker entrypoint for the Mezzanine
  rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.Execution,
      Mezzanine.Execution.ExecutionRecord,
      Mezzanine.Execution.LifecycleContinuation,
      Mezzanine.JobOutbox,
      Mezzanine.LowerGateway,
      Mezzanine.ExecutionDispatchWorker,
      Mezzanine.Installations,
      Mezzanine.WorkControl,
      Mezzanine.WorkQueries,
      Mezzanine.OperatorActions,
      Mezzanine.Reviews,
      Mezzanine.Execution.RuntimeStack
    ]
  end
end
