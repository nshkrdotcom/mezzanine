defmodule Mezzanine.Pack.Compiler do
  @moduledoc """
  Pure validation and compilation entrypoint for neutral Mezzanine domain packs.
  """

  alias Mezzanine.Pack.{
    CompiledPack,
    Diagnostics,
    Manifest,
    ManifestEmitter,
    Normalizer,
    SchemaValidator,
    ValidationError
  }

  @type pack_input :: term()
  @type compile_result :: {:ok, CompiledPack.t()} | {:error, [ValidationError.t()]}

  @spec compile(pack_input(), keyword()) :: compile_result()
  def compile(pack_or_manifest, opts \\ []) when is_list(opts) do
    with {:ok, manifest} <- load_manifest(pack_or_manifest) do
      diagnostics = SchemaValidator.diagnostics(manifest, opts)
      errors = Diagnostics.errors(diagnostics)

      case errors do
        [] ->
          manifest
          |> Normalizer.normalize()
          |> ManifestEmitter.emit()
          |> then(&{:ok, &1})

        _ ->
          {:error, errors}
      end
    end
  end

  @spec diagnostics(pack_input(), keyword()) :: [ValidationError.t()]
  def diagnostics(pack_or_manifest, opts \\ []) when is_list(opts) do
    case load_manifest(pack_or_manifest) do
      {:ok, manifest} -> SchemaValidator.diagnostics(manifest, opts)
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
