defmodule Mix.Tasks.Mezzanine.Substrate.Health do
  @moduledoc "Prints the Mezzanine-owned local substrate health contract."

  use Mix.Task

  alias Mezzanine.Substrate.Health

  @shortdoc "Prints local substrate health posture"

  @impl Mix.Task
  def run(args) do
    {opts, _argv, invalid} = OptionParser.parse(args, strict: [format: :string])

    if invalid != [] do
      Mix.raise("invalid options: #{inspect(invalid)}")
    end

    report = Health.report()

    case Health.validate_report(report) do
      :ok -> print(report, Keyword.get(opts, :format, "text"))
      {:error, failures} -> Mix.raise("mezzanine.substrate.health failed: #{inspect(failures)}")
    end
  end

  defp print(report, "text") do
    report
    |> Health.format()
    |> Mix.shell().info()
  end

  defp print(_report, format) do
    Mix.raise("unsupported format: #{format}")
  end
end
