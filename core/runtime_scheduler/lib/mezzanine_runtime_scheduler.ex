defmodule MezzanineRuntimeScheduler do
  @moduledoc """
  Runtime-scheduler facade for installation-scoped retry timing and restart
  recovery.
  """

  alias Mezzanine.RuntimeScheduler.{InstallationLease, InstallationLeaseStore, ReconcileOnStart}

  @spec components() :: [module()]
  def components do
    [InstallationLeaseStore, ReconcileOnStart]
  end

  @spec acquire_installation_lease(
          InstallationLease.t(),
          DateTime.t(),
          keyword()
        ) ::
          {:ok, :acquired | :renewed, InstallationLease.t()} | {:error, term()}
  def acquire_installation_lease(%InstallationLease{} = lease, now, opts \\ []) do
    InstallationLeaseStore.acquire_lease(lease, now, opts)
  end

  @spec fetch_installation_lease(String.t(), keyword()) ::
          {:ok, InstallationLease.t()} | :error
  def fetch_installation_lease(installation_id, opts \\ []) when is_binary(installation_id) do
    InstallationLeaseStore.fetch_current_lease(installation_id, opts)
  end

  @spec reconcile_on_start(String.t(), DateTime.t(), keyword()) ::
          {:ok, ReconcileOnStart.summary()} | {:error, term()}
  def reconcile_on_start(installation_id, now \\ DateTime.utc_now(), opts \\ []) do
    ReconcileOnStart.reconcile(installation_id, now, opts)
  end
end
