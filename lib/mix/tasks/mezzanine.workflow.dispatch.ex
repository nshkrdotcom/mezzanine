defmodule Mix.Tasks.Mezzanine.Workflow.Dispatch do
  use Mix.Task

  @moduledoc "Dispatches a Mezzanine workflow smoke command for Chassis integration checks."
  @shortdoc "Dispatch a Mezzanine workflow smoke command"

  @impl true
  def run([workflow | _args]) do
    Mix.shell().info(
      "workflow=#{workflow} status=dispatched receipt_ref=receipt:mezzanine:chassis:smoke"
    )
  end
end
