defmodule Mezzanine.Workflow.ChassisDeploymentWorkflow do
  @moduledoc "Mezzanine workflow facade for Chassis deployment materialization."
  def dispatch(attrs \\ %{}),
    do:
      {:ok,
       Map.merge(%{workflow: :chassis_deployment, receipt_ref: "receipt:deployment:smoke"}, attrs)}
end

defmodule Mezzanine.Workflow.ChassisRollbackWorkflow do
  @moduledoc "Mezzanine workflow facade for Chassis rollback."
  def dispatch(attrs \\ %{}),
    do: {:ok, Map.merge(%{workflow: :chassis_rollback, rollback_ref: "rollback:smoke"}, attrs)}
end

for workflow <- [
      FailureBatchWorkflow,
      CandidatePatchWorkflow,
      TrialReplayWorkflow,
      CandidateScoringWorkflow,
      PromotionConsentWorkflow,
      PromotionApplyWorkflow,
      SwapRollbackWorkflow,
      ModelMaterializationWorkflow,
      TensorPatchReloadWorkflow
    ] do
  defmodule Module.concat(Mezzanine.Workflow.Chassis.Evolution, workflow) do
    @moduledoc "Chassis Evolution workflow facade."
    def dispatch(attrs \\ %{}),
      do: {:ok, Map.merge(%{workflow: inspect(__MODULE__), state: :queued}, attrs)}
  end
end

for record <- [
      EvolutionIntentRecord,
      FailureBatchIntent,
      CandidatePromotionIntent,
      OperatorConsentRecord,
      ModelMaterializationIntent,
      TensorReloadIntent
    ] do
  defmodule Module.concat(Mezzanine.Chassis.Truth, record) do
    @moduledoc "Chassis Truth record."
    defstruct [:record_ref, :tenant_ref, :payload]
  end
end

defmodule Mezzanine.Read.ChassisDeploymentProjection do
  @moduledoc "Chassis deployment read projection."
  def last, do: %{status: :active, receipt_ref: "receipt:deployment:smoke"}
end

defmodule Mezzanine.Read.ChassisEvolutionProjection do
  @moduledoc "Chassis evolution read projection."
  def last, do: %{state: :queued, evolution_ref: "evo:dev:smoke"}
end
