defmodule Mezzanine.Pack.Compiler do
  @moduledoc """
  Pure validation and compilation entrypoint for neutral Mezzanine domain packs.
  """

  alias Mezzanine.Pack.{Builder, CompiledPack, Manifest, Normalizer, ValidationError, Validator}

  @type pack_input :: term()
  @type compile_result :: {:ok, CompiledPack.t()} | {:error, [ValidationError.t()]}

  @spec compile(pack_input()) :: compile_result()
  def compile(pack_or_manifest) do
    with {:ok, manifest} <- load_manifest(pack_or_manifest) do
      diagnostics = Validator.diagnostics(manifest)
      errors = Enum.filter(diagnostics, &(&1.severity == :error))

      case errors do
        [] ->
          manifest
          |> Normalizer.normalize()
          |> Builder.build()
          |> then(&{:ok, &1})

        _ ->
          {:error, errors}
      end
    end
  end

  @spec diagnostics(pack_input()) :: [ValidationError.t()]
  def diagnostics(pack_or_manifest) do
    case load_manifest(pack_or_manifest) do
      {:ok, manifest} -> Validator.diagnostics(manifest)
      {:error, issue} -> [issue]
    end
  end

  defp load_manifest(%Manifest{} = manifest), do: {:ok, manifest}

  defp load_manifest(module) when is_atom(module) do
    cond do
      not Code.ensure_loaded?(module) ->
        {:error,
         ValidationError.error([:pack_module], "pack module #{inspect(module)} is not available")}

      not function_exported?(module, :manifest, 0) ->
        {:error,
         ValidationError.error(
           [:pack_module],
           "pack module #{inspect(module)} does not export manifest/0"
         )}

      true ->
        case module.manifest() do
          %Manifest{} = manifest ->
            {:ok, manifest}

          other ->
            {:error,
             ValidationError.error(
               [:pack_module],
               "pack module #{inspect(module)} returned #{inspect(other)} instead of %Mezzanine.Pack.Manifest{}"
             )}
        end
    end
  end

  defp load_manifest(other) do
    {:error,
     ValidationError.error(
       [:manifest],
       "expected a pack module or %Mezzanine.Pack.Manifest{}, got: #{inspect(other)}"
     )}
  end
end
