defmodule Mezzanine.RuntimeScheduler.InstallationLeaseStoreTest do
  use Mezzanine.RuntimeScheduler.DataCase, async: false

  alias Mezzanine.RuntimeScheduler.{Fence, InstallationLease, InstallationLeaseStore}

  test "keeps ownership installation-scoped, fences competing owners, and allows takeover after expiry" do
    now = ~U[2026-04-16 13:00:00.000000Z]
    later = ~U[2026-04-16 13:10:00.000000Z]

    expense_lease = lease!("installation-expense", "scheduler-a", "lease-expense-a", 1, 7, now)
    invoice_lease = lease!("installation-invoice", "scheduler-b", "lease-invoice-b", 1, 3, now)

    assert {:ok, :acquired, ^expense_lease} =
             InstallationLeaseStore.acquire_lease(expense_lease, now)

    assert {:ok, :acquired, ^invoice_lease} =
             InstallationLeaseStore.acquire_lease(invoice_lease, now)

    assert {:error,
            {:held_by_other,
             %Fence{
               installation_id: "installation-expense",
               holder: "scheduler-a",
               lease_id: "lease-expense-a",
               epoch: 1,
               compiled_pack_revision: 7
             }}} =
             InstallationLeaseStore.acquire_lease(
               lease!("installation-expense", "scheduler-b", "lease-expense-b", 2, 7, now),
               now
             )

    assert {:error,
            {:stale_epoch,
             %Fence{
               installation_id: "installation-expense",
               holder: "scheduler-a",
               epoch: 1
             }}} =
             InstallationLeaseStore.acquire_lease(
               lease!("installation-expense", "scheduler-b", "lease-expense-b", 1, 7, later),
               later
             )

    takeover_lease =
      lease!("installation-expense", "scheduler-b", "lease-expense-b", 2, 7, later)

    assert {:ok, :acquired, ^takeover_lease} =
             InstallationLeaseStore.acquire_lease(takeover_lease, later)

    assert {:ok, fetched_expense} =
             InstallationLeaseStore.fetch_current_lease("installation-expense")

    assert fetched_expense == takeover_lease

    assert {:ok, fetched_invoice} =
             InstallationLeaseStore.fetch_current_lease("installation-invoice")

    assert fetched_invoice == invoice_lease
  end

  test "rejects stale compiled-pack revisions even after the prior lease expires" do
    now = ~U[2026-04-16 14:00:00.000000Z]
    later = ~U[2026-04-16 14:10:00.000000Z]

    current = lease!("installation-expense", "scheduler-a", "lease-expense-a", 2, 9, now)

    assert {:ok, :acquired, ^current} =
             InstallationLeaseStore.acquire_lease(current, now)

    assert {:error,
            {:stale_revision,
             %{
               attempted_revision: 8,
               current_revision: 9,
               fence: %Fence{
                 installation_id: "installation-expense",
                 holder: "scheduler-a",
                 lease_id: "lease-expense-a",
                 epoch: 2,
                 compiled_pack_revision: 9
               }
             }}} =
             InstallationLeaseStore.acquire_lease(
               lease!("installation-expense", "scheduler-b", "lease-expense-b", 3, 8, later),
               later
             )
  end

  defp lease!(installation_id, holder, lease_id, epoch, revision, now) do
    expires_at = DateTime.add(now, 300, :second)

    {:ok, lease} =
      InstallationLease.new(%{
        installation_id: installation_id,
        holder: holder,
        lease_id: lease_id,
        epoch: epoch,
        compiled_pack_revision: revision,
        expires_at: expires_at
      })

    lease
  end
end
