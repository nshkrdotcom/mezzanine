defmodule Mezzanine.AppKitBridge do
  @moduledoc """
  Optional AppKit backend adapters backed by Mezzanine service seams.
  """

  alias AppKit.Core.RunRef

  alias Mezzanine.AppKitBridge.{
    InstallationService,
    OperatorProjectionAdapter,
    ReviewActionService,
    ReviewQueryService,
    WorkControlAdapter
  }

  @spec start_run(map(), keyword()) :: {:ok, AppKit.Core.Result.t()} | {:error, atom()}
  defdelegate start_run(domain_call, opts \\ []), to: WorkControlAdapter

  @spec run_status(RunRef.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  defdelegate run_status(run_ref, attrs, opts \\ []), to: OperatorProjectionAdapter

  @spec review_run(RunRef.t(), map(), keyword()) :: {:ok, map()} | {:error, atom()}
  defdelegate review_run(run_ref, evidence_attrs, opts \\ []), to: OperatorProjectionAdapter

  @spec list_pending_reviews(String.t(), Ecto.UUID.t()) :: {:ok, [map()]} | {:error, term()}
  defdelegate list_pending_reviews(tenant_id, program_id), to: ReviewQueryService

  @spec get_review_detail(String.t(), Ecto.UUID.t()) :: {:ok, map()} | {:error, term()}
  defdelegate get_review_detail(tenant_id, review_unit_id), to: ReviewQueryService

  @spec record_review_decision(String.t(), Ecto.UUID.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate record_review_decision(tenant_id, review_unit_id, attrs, opts \\ []),
    to: ReviewActionService,
    as: :record_decision

  @spec create_installation(map(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate create_installation(attrs, opts \\ []), to: InstallationService

  @spec get_installation(Ecto.UUID.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate get_installation(installation_id, opts \\ []), to: InstallationService

  @spec list_installations(String.t(), map(), keyword()) :: {:ok, [map()]} | {:error, term()}
  defdelegate list_installations(tenant_id, filters \\ %{}, opts \\ []), to: InstallationService

  @spec update_installation_bindings(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, map()} | {:error, term()}
  defdelegate update_installation_bindings(installation_id, binding_config, opts \\ []),
    to: InstallationService,
    as: :update_bindings

  @spec suspend_installation(Ecto.UUID.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate suspend_installation(installation_id, opts \\ []), to: InstallationService

  @spec reactivate_installation(Ecto.UUID.t(), keyword()) :: {:ok, map()} | {:error, term()}
  defdelegate reactivate_installation(installation_id, opts \\ []), to: InstallationService
end
