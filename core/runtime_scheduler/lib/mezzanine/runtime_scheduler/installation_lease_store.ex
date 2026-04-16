defmodule Mezzanine.RuntimeScheduler.InstallationLeaseStore do
  @moduledoc """
  Canonical durable lease store for installation-scoped runtime ownership.
  """

  import Ecto.Query

  alias Mezzanine.RuntimeScheduler.{Fence, InstallationLease, Repo}
  alias Mezzanine.RuntimeScheduler.Schemas.InstallationRuntimeLease

  @spec acquire_lease(InstallationLease.t(), DateTime.t(), keyword()) ::
          {:ok, :acquired | :renewed, InstallationLease.t()} | {:error, term()}
  def acquire_lease(%InstallationLease{} = candidate, %DateTime{} = now, opts \\ []) do
    repo = repo(opts)

    case repo.transaction(fn -> do_acquire_lease(repo, candidate, now) end) do
      {:ok, {:ok, status, lease}} -> {:ok, status, lease}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec fetch_current_lease(String.t(), keyword()) :: {:ok, InstallationLease.t()} | :error
  def fetch_current_lease(installation_id, opts \\ []) when is_binary(installation_id) do
    repo = repo(opts)

    case repo.get(InstallationRuntimeLease, installation_id) do
      nil -> :error
      lease -> {:ok, schema_to_lease(lease)}
    end
  end

  defp do_acquire_lease(repo, candidate, now) do
    current =
      InstallationRuntimeLease
      |> where([lease], lease.installation_id == ^candidate.installation_id)
      |> lock("FOR UPDATE")
      |> repo.one()

    case current do
      nil ->
        persist_new_lease(repo, candidate, :acquired)

      %InstallationRuntimeLease{} = persisted ->
        current_lease = schema_to_lease(persisted)

        cond do
          same_lease?(current_lease, candidate) ->
            persist_existing_lease(repo, persisted, candidate, :renewed)

          stale_revision?(current_lease, candidate) ->
            repo.rollback({:stale_revision, Fence.from_lease(current_lease)})

          InstallationLease.expired?(current_lease, now) and candidate.epoch > current_lease.epoch ->
            persist_existing_lease(repo, persisted, candidate, :acquired)

          InstallationLease.expired?(current_lease, now) ->
            repo.rollback({:stale_epoch, Fence.from_lease(current_lease)})

          true ->
            repo.rollback({:held_by_other, Fence.from_lease(current_lease)})
        end
    end
  end

  defp persist_new_lease(repo, candidate, status) do
    changeset =
      InstallationRuntimeLease.changeset(%InstallationRuntimeLease{}, %{
        installation_id: candidate.installation_id,
        holder: candidate.holder,
        lease_id: candidate.lease_id,
        epoch: candidate.epoch,
        compiled_pack_revision: candidate.compiled_pack_revision,
        expires_at: candidate.expires_at
      })

    case repo.insert(changeset) do
      {:ok, _schema} -> {:ok, status, candidate}
      {:error, changeset} -> repo.rollback(changeset)
    end
  end

  defp persist_existing_lease(repo, persisted, candidate, status) do
    changeset =
      InstallationRuntimeLease.changeset(persisted, %{
        holder: candidate.holder,
        lease_id: candidate.lease_id,
        epoch: candidate.epoch,
        compiled_pack_revision: candidate.compiled_pack_revision,
        expires_at: candidate.expires_at
      })

    case repo.update(changeset) do
      {:ok, _schema} -> {:ok, status, candidate}
      {:error, changeset} -> repo.rollback(changeset)
    end
  end

  defp same_lease?(current, candidate) do
    current.holder == candidate.holder and current.lease_id == candidate.lease_id and
      current.epoch == candidate.epoch and
      current.compiled_pack_revision == candidate.compiled_pack_revision
  end

  defp stale_revision?(current, candidate) do
    candidate.compiled_pack_revision < current.compiled_pack_revision
  end

  defp schema_to_lease(schema) do
    %InstallationLease{
      installation_id: schema.installation_id,
      holder: schema.holder,
      lease_id: schema.lease_id,
      epoch: schema.epoch,
      compiled_pack_revision: schema.compiled_pack_revision,
      expires_at: schema.expires_at
    }
  end

  defp repo(opts), do: Keyword.get(opts, :repo, Repo)
end
