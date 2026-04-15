defmodule Mezzanine.Scheduler.WorkSelector do
  @moduledoc """
  Queries durable work that is ready for scheduler dispatch.
  """

  require Ash.Query

  alias Mezzanine.Work.WorkObject

  @spec ready_work(String.t(), DateTime.t()) :: {:ok, [struct()]} | {:error, term()}
  def ready_work(tenant_id, now \\ DateTime.utc_now()) do
    actor = %{tenant_id: tenant_id}

    WorkObject
    |> Ash.Query.set_tenant(tenant_id)
    |> Ash.Query.filter(
      status == :planned and is_nil(blocked_by_work_id) and not is_nil(current_plan_id) and
        (is_nil(lease_expires_at) or lease_expires_at <= ^now)
    )
    |> Ash.read(actor: actor, domain: Mezzanine.Work)
  end
end
