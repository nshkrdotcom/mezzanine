defmodule MezzanineExecutionEngine do
  @moduledoc """
  Neutral execution-ledger contract entrypoint for the Mezzanine rebuild.
  """

  @spec components() :: [module()]
  def components do
    [
      Mezzanine.Execution,
      Mezzanine.Execution.ExecutionRecord,
      Mezzanine.Execution.DispatchOutboxEntry,
      Mezzanine.Execution.Dispatcher,
      Mezzanine.Installations,
      Mezzanine.WorkControl,
      Mezzanine.WorkQueries,
      Mezzanine.OperatorActions,
      Mezzanine.Reviews,
      Mezzanine.Execution.RuntimeStack
    ]
  end
end
