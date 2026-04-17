defmodule Mezzanine.Policy.BundleLoader do
  @moduledoc """
  Loads pure policy bundles from strings, files, or maps.
  """

  alias Mezzanine.Policy.Compiler
  alias Mezzanine.Policy.FrontmatterParser
  alias MezzanineOpsModel.PolicyBundle

  @type fallback_result :: {:fallback, PolicyBundle.t(), term()}

  @spec load_string(String.t(), keyword()) :: {:ok, PolicyBundle.t()} | {:error, term()}
  def load_string(content, opts \\ []) when is_binary(content) do
    case FrontmatterParser.parse(content) do
      {:ok, parsed} ->
        PolicyBundle.new(%{
          bundle_id:
            Keyword.get(
              opts,
              :bundle_id,
              default_bundle_id(Keyword.get(opts, :source_ref, "inline"))
            ),
          source_ref: Keyword.get(opts, :source_ref, "inline"),
          config: parsed.config,
          prompt_template: parsed.prompt_template
        })

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec load_file(Path.t(), keyword()) :: {:ok, PolicyBundle.t()} | {:error, term()}
  def load_file(path, opts \\ []) when is_binary(path) do
    case File.read(path) do
      {:ok, content} ->
        load_string(
          content,
          opts
          |> Keyword.put_new(:source_ref, path)
          |> Keyword.put_new(:bundle_id, default_bundle_id(Path.basename(path, ".md")))
        )

      {:error, reason} ->
        {:error, {:missing_policy_file, path, reason}}
    end
  end

  @spec load_map(map(), keyword()) :: {:ok, PolicyBundle.t()} | {:error, term()}
  def load_map(%{} = payload, opts \\ []) do
    PolicyBundle.new(%{
      bundle_id: Keyword.get(opts, :bundle_id, default_bundle_id("inline_map")),
      source_ref: Keyword.get(opts, :source_ref, "inline_map"),
      config: Map.get(payload, :config) || Map.get(payload, "config", %{}),
      prompt_template:
        Map.get(payload, :prompt_template) || Map.get(payload, "prompt_template", "")
    })
  end

  @spec reload_string(PolicyBundle.t(), String.t(), keyword()) ::
          {:ok, PolicyBundle.t()} | fallback_result()
  def reload_string(%PolicyBundle{} = previous_bundle, content, opts \\ []) do
    case load_string(content, opts) do
      {:ok, bundle} ->
        case Compiler.compile(bundle) do
          {:ok, compiled_bundle} -> {:ok, compiled_bundle}
          {:error, reason} -> {:fallback, previous_bundle, reason}
        end

      {:error, reason} ->
        {:fallback, previous_bundle, reason}
    end
  end

  @spec reload_file(PolicyBundle.t(), Path.t(), keyword()) ::
          {:ok, PolicyBundle.t()} | fallback_result()
  def reload_file(%PolicyBundle{} = previous_bundle, path, opts \\ []) do
    case load_file(path, opts) do
      {:ok, bundle} ->
        case Compiler.compile(bundle) do
          {:ok, compiled_bundle} -> {:ok, compiled_bundle}
          {:error, reason} -> {:fallback, previous_bundle, reason}
        end

      {:error, reason} ->
        {:fallback, previous_bundle, reason}
    end
  end

  defp default_bundle_id(source_ref) do
    "policy:" <> Integer.to_string(:erlang.phash2(source_ref))
  end
end
