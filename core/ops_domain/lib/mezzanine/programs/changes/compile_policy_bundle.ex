defmodule Mezzanine.Programs.Changes.CompilePolicyBundle do
  @moduledoc false

  use Ash.Resource.Change

  alias Mezzanine.Policy.{BundleLoader, Compiler}
  alias MezzanineOpsModel.Codec

  @impl true
  def change(changeset, opts, _context) do
    fallback_on_error? = Keyword.get(opts, :fallback_on_error?, false)

    Ash.Changeset.before_action(changeset, fn changeset ->
      body = Ash.Changeset.get_attribute(changeset, :body)
      source_ref = Ash.Changeset.get_attribute(changeset, :source_ref) || "inline"
      policy_kind = Ash.Changeset.get_attribute(changeset, :policy_kind)
      metadata = Ash.Changeset.get_attribute(changeset, :metadata) || %{}

      with {:ok, bundle} <- load_bundle(policy_kind, body, source_ref, metadata),
           {:ok, compiled_bundle} <- Compiler.compile(bundle) do
        changeset
        |> Ash.Changeset.force_change_attribute(:source_ref, compiled_bundle.source_ref)
        |> Ash.Changeset.force_change_attribute(:config, Codec.dump(compiled_bundle.config))
        |> Ash.Changeset.force_change_attribute(:prompt_template, compiled_bundle.prompt_template)
        |> Ash.Changeset.force_change_attribute(
          :compiled_form,
          Codec.dump(compiled_bundle.compiled_form)
        )
        |> Ash.Changeset.force_change_attribute(:status, :compiled)
      else
        {:error, reason} when fallback_on_error? ->
          changeset
          |> Ash.Changeset.force_change_attribute(:status, :stale_on_error)
          |> Ash.Changeset.force_change_attribute(:metadata, stale_metadata(changeset, reason))

        {:error, reason} ->
          Ash.Changeset.add_error(changeset, field: :body, message: inspect(reason))
      end
    end)
  end

  defp stale_metadata(changeset, reason) do
    changeset
    |> Ash.Changeset.get_attribute(:metadata)
    |> Kernel.||(%{})
    |> Map.put("compile_error", inspect(reason))
  end

  defp load_bundle(:structured_config, body, source_ref, metadata) do
    BundleLoader.load_map(
      %{
        "config" => structured_config(metadata),
        "prompt_template" => body
      },
      source_ref: source_ref
    )
  end

  defp load_bundle(_policy_kind, body, source_ref, _metadata) do
    BundleLoader.load_string(body, source_ref: source_ref)
  end

  defp structured_config(metadata) when is_map(metadata) do
    Map.get(metadata, "runtime_policy_config") ||
      Map.get(metadata, :runtime_policy_config) ||
      Map.get(metadata, "config") ||
      Map.get(metadata, :config) ||
      %{}
  end

  defp structured_config(_metadata), do: %{}
end
