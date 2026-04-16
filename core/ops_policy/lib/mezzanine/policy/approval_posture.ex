defmodule Mezzanine.Policy.ApprovalPosture do
  @moduledoc """
  Typed approval-posture compiler.
  """

  alias Mezzanine.Policy.Helpers

  @type mode :: :manual | :auto | :escalated
  @type t :: %{mode: mode(), reviewers: [String.t()], escalation_required: boolean()}

  @spec from_config(map()) :: {:ok, t()} | {:error, {:invalid_approval_mode, term()}}
  def from_config(config) do
    approval = Helpers.section(config, :approval)

    with {:ok, mode} <- cast_mode(Helpers.value(approval, :mode, "manual")) do
      {:ok,
       %{
         mode: mode,
         reviewers: Helpers.string_list(Helpers.value(approval, :reviewers, [])),
         escalation_required:
           Helpers.boolean(Helpers.value(approval, :escalation_required, false), false)
       }}
    end
  end

  defp cast_mode(value) when value in [:manual, :auto, :escalated], do: {:ok, value}
  defp cast_mode("manual"), do: {:ok, :manual}
  defp cast_mode("auto"), do: {:ok, :auto}
  defp cast_mode("escalated"), do: {:ok, :escalated}
  defp cast_mode(value), do: {:error, {:invalid_approval_mode, value}}
end
