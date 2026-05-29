defmodule Mix.Tasks.Mezzanine.Read.Get do
  use Mix.Task

  @moduledoc "Reads a Mezzanine projection smoke row for Chassis integration checks."
  @shortdoc "Read a Mezzanine projection smoke row"

  @impl true
  def run([projection | _args]) do
    Mix.shell().info(
      "projection=#{projection} status=active receipt_ref=receipt:mezzanine:read:smoke"
    )
  end
end
