defmodule Mix.Tasks.Mezzanine.Read.Get do
  use Mix.Task

  @moduledoc "Reads Mezzanine projections from configured read stores."
  @shortdoc "Read a Mezzanine projection"

  @impl true
  def run(["chassis_deployment_projection" | args]) do
    Mix.Task.run("app.start")
    query = attrs(args)

    {:file, Mezzanine.Read.ChassisDeploymentProjection.default_file()}
    |> Mezzanine.Read.ChassisDeploymentProjection.latest(query)
    |> case do
      {:ok, projection} ->
        Mix.shell().info(
          "projection=chassis_deployment_projection status=#{projection.status} receipt_ref=#{projection.receipt_ref} app_ref=#{projection.app_ref} tenant_ref=#{projection.tenant_ref} installation_ref=#{projection.installation_ref}"
        )

      {:error, reason} ->
        Mix.raise("chassis_deployment_projection read failed: #{inspect(reason)}")
    end
  end

  def run([projection | _args]),
    do: Mix.raise("unsupported Mezzanine projection #{inspect(projection)}")

  def run([]), do: Mix.raise("projection name is required")

  defp attrs(args), do: parse_args(args, [])

  defp parse_args(["--tenant-ref", value | rest], acc),
    do: parse_args(rest, Keyword.put(acc, :tenant_ref, value))

  defp parse_args(["--installation-ref", value | rest], acc),
    do: parse_args(rest, Keyword.put(acc, :installation_ref, value))

  defp parse_args([unknown | _rest], _acc), do: Mix.raise("unsupported option #{unknown}")
  defp parse_args([], acc), do: Enum.reverse(acc)
end
