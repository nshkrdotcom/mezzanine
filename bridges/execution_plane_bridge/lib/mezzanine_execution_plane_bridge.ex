defmodule Mezzanine.ExecutionPlaneBridge do
  @moduledoc """
  Honest placeholder for the future execution-plane boundary.
  """

  @type intent_type :: :run | :effect | :read
  @type not_supported_error ::
          {:not_supported, %{bridge: :execution_plane, intent_type: intent_type()}}

  @spec dispatch_run(MezzanineOpsModel.Intent.RunIntent.t(), keyword()) ::
          {:error, not_supported_error()}
  def dispatch_run(_intent, _opts \\ []), do: {:error, not_supported(:run)}

  @spec dispatch_effect(MezzanineOpsModel.Intent.EffectIntent.t(), keyword()) ::
          {:error, not_supported_error()}
  def dispatch_effect(_intent, _opts \\ []), do: {:error, not_supported(:effect)}

  @spec dispatch_read(MezzanineOpsModel.Intent.ReadIntent.t(), keyword()) ::
          {:error, not_supported_error()}
  def dispatch_read(_intent, _opts \\ []), do: {:error, not_supported(:read)}

  defp not_supported(intent_type) do
    {:not_supported, %{bridge: :execution_plane, intent_type: intent_type}}
  end
end
