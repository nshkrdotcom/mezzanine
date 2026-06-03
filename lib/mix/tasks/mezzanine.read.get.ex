defmodule Mix.Tasks.Mezzanine.Read.Get do
  use Mix.Task

  @moduledoc "Reads Mezzanine projections from configured read stores."
  @shortdoc "Read a Mezzanine projection"

  alias Mezzanine.Read.ChassisDeploymentProjection
  alias Mezzanine.Read.ChassisEvolutionProjection

  @impl true
  def run(["chassis_deployment_projection" | args]) do
    Mix.Task.run("app.start")
    query = attrs(args)

    {:file, ChassisDeploymentProjection.default_file()}
    |> ChassisDeploymentProjection.latest(query)
    |> case do
      {:ok, projection} ->
        Mix.shell().info(
          "projection=chassis_deployment_projection status=#{projection.status} receipt_ref=#{projection.receipt_ref} app_ref=#{projection.app_ref} tenant_ref=#{projection.tenant_ref} installation_ref=#{projection.installation_ref}"
        )

      {:error, reason} ->
        Mix.raise("chassis_deployment_projection read failed: #{inspect(reason)}")
    end
  end

  def run(["chassis_evolution" | args]) do
    Mix.Task.run("app.start")
    query = attrs(args)

    rows =
      ChassisEvolutionProjection.default_file()
      |> then(&{:file, &1})
      |> ChassisEvolutionProjection.list()
      |> filter_rows(Keyword.delete(query, :last))

    last = Keyword.get(query, :last, 1)

    rows
    |> Enum.take(last)
    |> case do
      [] ->
        Mix.raise("chassis_evolution read failed: :not_found")

      rows ->
        Enum.each(rows, fn row ->
          Mix.shell().info(
            "projection=chassis_evolution kind=#{row.projection} primary_ref=#{row.primary_ref} state_or_outcome=#{row.state_or_outcome} tenant_ref=#{row.tenant_ref} installation_ref=#{row.installation_ref}"
          )
        end)
    end
  end

  def run([projection | _args]),
    do: Mix.raise("unsupported Mezzanine projection #{inspect(projection)}")

  def run([]), do: Mix.raise("projection name is required")

  defp attrs(args), do: parse_args(args, [])

  defp parse_args(["--tenant-ref", value | rest], acc),
    do: parse_args(rest, Keyword.put(acc, :tenant_ref, value))

  defp parse_args(["--tenant", value | rest], acc),
    do: parse_args(rest, Keyword.put(acc, :tenant_ref, value))

  defp parse_args(["--installation-ref", value | rest], acc),
    do: parse_args(rest, Keyword.put(acc, :installation_ref, value))

  defp parse_args(["--installation", value | rest], acc),
    do: parse_args(rest, Keyword.put(acc, :installation_ref, value))

  defp parse_args(["--projection", value | rest], acc),
    do: parse_args(rest, Keyword.put(acc, :projection, existing_atom!(value)))

  defp parse_args(["--last", value | rest], acc),
    do: parse_args(rest, Keyword.put(acc, :last, parse_positive_integer!(value)))

  defp parse_args([unknown | _rest], _acc), do: Mix.raise("unsupported option #{unknown}")
  defp parse_args([], acc), do: Enum.reverse(acc)

  defp filter_rows(rows, query) do
    rows
    |> Enum.reverse()
    |> Enum.filter(fn row ->
      Enum.all?(query, fn {key, expected} -> Map.get(row, key) == expected end)
    end)
  end

  defp existing_atom!(value) do
    String.to_existing_atom(value)
  rescue
    ArgumentError -> Mix.raise("unsupported projection #{inspect(value)}")
  end

  defp parse_positive_integer!(value) do
    case Integer.parse(value) do
      {integer, ""} when integer > 0 -> integer
      _other -> Mix.raise("--last must be a positive integer")
    end
  end
end
