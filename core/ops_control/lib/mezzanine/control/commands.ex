defmodule Mezzanine.Control.Commands do
  @moduledoc """
  Deprecated compatibility shim over `Mezzanine.OperatorActions`.
  """

  defdelegate pause_work(tenant_id, work_object_id, operator_ref, payload \\ %{}),
    to: Mezzanine.OperatorActions

  defdelegate resume_work(tenant_id, work_object_id, operator_ref, payload \\ %{}),
    to: Mezzanine.OperatorActions

  defdelegate cancel_work(tenant_id, work_object_id, operator_ref, payload \\ %{}),
    to: Mezzanine.OperatorActions

  defdelegate request_replan(tenant_id, work_object_id, operator_ref, payload \\ %{}),
    to: Mezzanine.OperatorActions

  defdelegate override_grant_profile(tenant_id, work_object_id, operator_ref, override_set),
    to: Mezzanine.OperatorActions
end
