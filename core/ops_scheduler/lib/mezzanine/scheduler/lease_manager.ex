defmodule Mezzanine.Scheduler.LeaseManager do
  @moduledoc """
  Atomic durable lease claims for schedulable work.
  """

  import Ecto.Query

  require Ash.Query

  alias Mezzanine.OpsDomain.Repo
  alias Mezzanine.Work.WorkObject

  @spec claim(String.t(), Ecto.UUID.t(), String.t(), pos_integer(), DateTime.t()) ::
          {:ok, struct()} | {:error, :already_claimed | :not_found | :not_schedulable}
  def claim(tenant_id, work_object_id, lease_owner, ttl_ms, now \\ DateTime.utc_now())
      when is_integer(ttl_ms) and ttl_ms > 0 do
    expires_at = DateTime.add(now, ttl_ms, :millisecond)

    do_claim(tenant_id, work_object_id, lease_owner, expires_at, now)
  end

  defp do_claim(tenant_id, work_object_id, lease_owner, expires_at, now) do
    case Repo.update_all(
           claimable_query(tenant_id, work_object_id, lease_owner, now),
           set: [lease_owner: lease_owner, lease_expires_at: expires_at, updated_at: now]
         ) do
      {1, _} ->
        fetch_work(tenant_id, work_object_id)

      {0, _} ->
        unavailable_reason(tenant_id, work_object_id, now)
    end
  end

  defp claimable_query(tenant_id, work_object_id, lease_owner, now) do
    from(work in "work_objects",
      where:
        field(work, :tenant_id) == ^tenant_id and
          field(work, :id) == type(^work_object_id, :binary_id) and
          field(work, :status) == ^"planned" and
          (is_nil(field(work, :lease_owner)) or is_nil(field(work, :lease_expires_at)) or
             field(work, :lease_expires_at) < ^now or field(work, :lease_owner) == ^lease_owner)
    )
  end

  defp unavailable_reason(tenant_id, work_object_id, now) do
    case fetch_work(tenant_id, work_object_id) do
      {:ok, %WorkObject{status: :planned} = work_object} ->
        if available_lease?(work_object, now) do
          {:error, :not_found}
        else
          {:error, :already_claimed}
        end

      {:ok, %WorkObject{}} ->
        {:error, :not_schedulable}

      {:error, :not_found} = error ->
        error
    end
  end

  defp available_lease?(%WorkObject{lease_owner: nil}, _now), do: true
  defp available_lease?(%WorkObject{lease_expires_at: nil}, _now), do: true

  defp available_lease?(%WorkObject{lease_expires_at: lease_expires_at}, now) do
    DateTime.compare(lease_expires_at, now) == :lt
  end

  defp fetch_work(tenant_id, work_object_id) do
    actor = %{tenant_id: tenant_id}

    case WorkObject
         |> Ash.Query.set_tenant(tenant_id)
         |> Ash.Query.filter(id == ^work_object_id)
         |> Ash.read(actor: actor, domain: Mezzanine.Work) do
      {:ok, [work_object]} -> {:ok, work_object}
      {:ok, []} -> {:error, :not_found}
      {:error, error} -> {:error, error}
    end
  end
end
