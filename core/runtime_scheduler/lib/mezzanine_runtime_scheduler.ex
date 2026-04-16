defmodule MezzanineRuntimeScheduler do
  @moduledoc """
  Runtime-scheduler facade for installation-scoped retry timing and restart
  recovery.
  """

  alias Mezzanine.RuntimeScheduler.ReconcileOnStart

  @spec components() :: [module()]
  def components do
    [ReconcileOnStart]
  end

  @spec reconcile_on_start(String.t(), DateTime.t(), keyword()) ::
          {:ok, ReconcileOnStart.summary()} | {:error, term()}
  def reconcile_on_start(installation_id, now \\ DateTime.utc_now(), opts \\ []) do
    ReconcileOnStart.reconcile(installation_id, now, opts)
  end
end
