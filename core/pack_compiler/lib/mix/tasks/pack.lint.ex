defmodule Mix.Tasks.Pack.Lint do
  use Mix.Task

  alias Mezzanine.Pack.Compiler

  @shortdoc "Lint a Mezzanine pack module"

  @moduledoc """
  Validates a pack module and exits non-zero when the manifest is invalid.

      mix pack.lint Mezzanine.TestPacks.ExpenseApprovalPack
  """

  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(args) do
    Mix.Task.run("compile", [])

    module =
      case args do
        [module_name] ->
          module_name |> String.split(".") |> Enum.map(&String.to_atom/1) |> Module.concat()

        _other ->
          Mix.raise("usage: mix pack.lint Elixir.Module.Name")
      end

    diagnostics = Compiler.diagnostics(module)

    Enum.each(diagnostics, fn diagnostic ->
      Mix.shell().info(format_diagnostic(diagnostic))
    end)

    if Enum.any?(diagnostics, &(&1.severity == :error)) do
      Mix.raise("pack lint failed for #{inspect(module)}")
    else
      Mix.shell().info("pack lint passed for #{inspect(module)}")
      :ok
    end
  end

  defp format_diagnostic(%{severity: severity, path: path, message: message}) do
    "#{severity}: #{format_path(path)} #{message}"
  end

  defp format_path([]), do: "<root>"
  defp format_path(path), do: Enum.map_join(path, ".", &to_string/1)
end
