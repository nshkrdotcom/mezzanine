defmodule Mezzanine.Assurance do
  @moduledoc """
  Compatibility shim over `Mezzanine.Reviews`.
  """

  defdelegate list_pending_reviews(tenant_id), to: Mezzanine.Reviews
  defdelegate review_detail(tenant_id, review_unit_id), to: Mezzanine.Reviews
  defdelegate gate_status(tenant_id, work_object_id), to: Mezzanine.Reviews
  defdelegate release_ready?(tenant_id, work_object_id), to: Mezzanine.Reviews
  defdelegate record_decision(tenant_id, review_unit_id, attrs), to: Mezzanine.Reviews
  defdelegate waive_review(tenant_id, review_unit_id, attrs), to: Mezzanine.Reviews
  defdelegate escalate_review(tenant_id, review_unit_id, attrs), to: Mezzanine.Reviews
end
