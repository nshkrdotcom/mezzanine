defmodule Mezzanine.AppKitBridge do
  @moduledoc """
  Optional AppKit backend adapters backed by Mezzanine service seams.
  """

  alias AppKit.Core.RunRef
  alias Mezzanine.AppKitBridge.{OperatorProjectionAdapter, WorkControlAdapter}

  @spec start_run(map(), keyword()) :: {:ok, AppKit.Core.Result.t()} | {:error, atom()}
  defdelegate start_run(domain_call, opts \\ []), to: WorkControlAdapter

  @spec run_status(RunRef.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  defdelegate run_status(run_ref, attrs, opts \\ []), to: OperatorProjectionAdapter

  @spec review_run(RunRef.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  defdelegate review_run(run_ref, evidence_attrs, opts \\ []), to: OperatorProjectionAdapter
end
