defmodule Mezzanine.WorkspaceEngine.Allocator do
  @moduledoc """
  Deterministic local workspace reservation helpers.
  """

  alias Mezzanine.WorkspaceEngine.{PathSafety, WorkspaceRecord}

  @required_fields [:installation_id, :subject_id, :workspace_root]

  @spec reserve(map()) ::
          {:ok, WorkspaceRecord.t()} | {:error, atom() | {:missing_required, atom()}}
  def reserve(attrs) when is_map(attrs) do
    with :ok <- validate_required(attrs),
         {:ok, root} <- ensure_root(value(attrs, :workspace_root)),
         slug = PathSafety.slug(value(attrs, :subject_ref) || value(attrs, :subject_id)),
         path = Path.join(root, slug),
         reuse? = File.dir?(path),
         :ok <- PathSafety.prepare_directory(root, path) do
      {:ok, build_record(attrs, root, path, slug, reuse?)}
    end
  end

  defp build_record(attrs, root, path, slug, reuse?) do
    installation_id = value(attrs, :installation_id)
    subject_id = value(attrs, :subject_id)
    cleanup_policy = value(attrs, :cleanup_policy) || :never
    logical_ref = "workspace:#{installation_id}:#{subject_id}"
    safety_metadata = %{cleanup_policy: cleanup_policy, placement_kind: placement_kind(attrs)}

    %WorkspaceRecord{
      workspace_id: "wks_" <> digest([logical_ref, path], 24),
      installation_id: installation_id,
      subject_id: subject_id,
      subject_ref: value(attrs, :subject_ref),
      logical_ref: logical_ref,
      concrete_root: root,
      concrete_path: path,
      slug: slug,
      placement_kind: placement_kind(attrs),
      cleanup_policy: cleanup_policy,
      safety_hash: PathSafety.safety_hash(root, path, safety_metadata),
      file_scope: value(attrs, :file_scope) || %{writable_roots: [path], read_roots: [path]},
      hook_specs: value(attrs, :hook_specs) || [],
      remote_hints: value(attrs, :remote_hints) || %{},
      reuse?: reuse?
    }
  end

  defp ensure_root(root) do
    root = Path.expand(root)

    case File.mkdir_p(root) do
      :ok -> {:ok, root}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_required(attrs) do
    case Enum.find(@required_fields, &blank?(value(attrs, &1))) do
      nil -> :ok
      field -> {:error, {:missing_required, field}}
    end
  end

  defp placement_kind(attrs), do: value(attrs, :placement_kind) || :local
  defp value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, Atom.to_string(key))
  defp blank?(value), do: value in [nil, ""]

  defp digest(value, length) do
    value
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> binary_part(0, length)
  end
end
