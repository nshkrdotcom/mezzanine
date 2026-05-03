Code.require_file("weld.exs", __DIR__)
Code.require_file("workspace_contract.exs", __DIR__)
Code.require_file("internal_modularity_contract.exs", __DIR__)

defmodule Mezzanine.Build.ProjectedArtifactFidelity do
  @moduledoc false

  alias Mezzanine.Build.InternalModularityContract
  alias Mezzanine.Build.WeldContract
  alias Mezzanine.Build.WorkspaceContract

  @artifact_id "mezzanine_core"
  @default_projection_path "dist/hex/mezzanine_core"
  @release_manifest_ref "mezzanine_projected_artifact_fidelity_profiles[0]"
  @required_fields [
    :artifact_id,
    :source_repo_ref,
    :source_root_ref_set,
    :weld_manifest_ref,
    :weld_manifest_hash,
    :workspace_contract_ref,
    :internal_modularity_contract_ref,
    :weld_verify_or_packaging_test_ref,
    :projection_branch_or_archive_ref,
    :artifact_hash_manifest_ref,
    :manual_patch_disposition,
    :owner_approval_ref_when_manual_patch,
    :release_manifest_ref
  ]
  @manual_patch_dispositions [
    "none",
    "generated_from_source_roots",
    "owner_approved_emergency_patch_with_backport",
    "blocked_manual_edit"
  ]
  @manual_patch_requires_owner_approval "owner_approved_emergency_patch_with_backport"
  @no_manual_patch_owner_ref "not_required_no_manual_patch"
  @missing_projection_ref "missing_projection_archive_run_mix_weld_verify"

  @type profile :: %{required(atom()) => term()}

  @spec required_fields() :: [atom()]
  def required_fields, do: @required_fields

  @spec profile(keyword()) :: profile()
  def profile(opts \\ []) do
    root = opts |> Keyword.get(:root, default_root()) |> Path.expand()
    projection_path = Keyword.get(opts, :projection_path, @default_projection_path)
    source_roots = artifact_roots()
    projection_ref = projection_ref(root, projection_path)

    %{
      artifact_id: @artifact_id,
      source_repo_ref: git_ref(root),
      source_root_ref_set: source_root_ref_set(root, source_roots),
      weld_manifest_ref: "build_support/weld.exs",
      weld_manifest_hash: file_hash_ref(root, "build_support/weld.exs"),
      workspace_contract_ref: contract_ref(root, "build_support/workspace_contract.exs"),
      internal_modularity_contract_ref:
        contract_ref(root, "build_support/internal_modularity_contract.exs"),
      weld_verify_or_packaging_test_ref: "mix weld.verify -> packaging/weld/mezzanine_core/test",
      projection_branch_or_archive_ref: projection_ref.projection_branch_or_archive_ref,
      artifact_hash_manifest_ref: projection_ref.artifact_hash_manifest_ref,
      manual_patch_disposition: "generated_from_source_roots",
      owner_approval_ref_when_manual_patch: @no_manual_patch_owner_ref,
      release_manifest_ref: @release_manifest_ref,
      workspace_project_globs: WorkspaceContract.active_project_globs(),
      internal_modularity_package_paths: InternalModularityContract.package_paths(),
      source_root_hash_manifest_ref: hash_manifest_ref(root, source_roots),
      feature_freeze_rule:
        "projected artifact output is generated from Weld source roots and must not be edited as canonical source"
    }
  end

  @spec validate(profile(), keyword()) :: :ok | {:error, [term()]}
  def validate(profile, opts \\ []) when is_map(profile) do
    require_projection? = Keyword.get(opts, :require_projection?, false)

    []
    |> require_fields(profile)
    |> require_source_roots(profile)
    |> require_manifest_refs(profile)
    |> require_manual_patch_policy(profile)
    |> require_projection_hash(profile, require_projection?)
    |> case do
      [] -> :ok
      errors -> {:error, Enum.reverse(errors)}
    end
  end

  @spec validate!(profile(), keyword()) :: :ok
  def validate!(profile, opts \\ []) do
    case validate(profile, opts) do
      :ok ->
        :ok

      {:error, errors} ->
        raise ArgumentError, "projected artifact fidelity failed: #{inspect(errors)}"
    end
  end

  defp require_fields(errors, profile) do
    missing =
      @required_fields
      |> Enum.reject(fn field ->
        case Map.get(profile, field) do
          nil -> false
          "" -> false
          [] -> false
          _present -> true
        end
      end)

    case missing do
      [] -> errors
      _ -> [{:missing_required_fields, missing} | errors]
    end
  end

  defp require_source_roots(errors, profile) do
    actual_roots =
      profile
      |> Map.get(:source_root_ref_set, [])
      |> Enum.map(&Map.get(&1, :path))
      |> Enum.reject(&is_nil/1)

    expected_roots = artifact_roots()

    cond do
      actual_roots != expected_roots ->
        [{:source_roots_do_not_match_weld_contract, expected_roots, actual_roots} | errors]

      Enum.any?(
        Map.get(profile, :source_root_ref_set, []),
        &(&1.hash_ref == @missing_projection_ref)
      ) ->
        [{:source_root_missing, actual_roots} | errors]

      true ->
        errors
    end
  end

  defp require_manifest_refs(errors, profile) do
    refs = [
      Map.get(profile, :weld_manifest_hash),
      Map.get(profile, :workspace_contract_ref),
      Map.get(profile, :internal_modularity_contract_ref),
      Map.get(profile, :source_root_hash_manifest_ref)
    ]

    if Enum.all?(refs, &ref_has_sha256?/1), do: errors, else: [:invalid_hash_ref | errors]
  end

  defp require_manual_patch_policy(errors, profile) do
    disposition = Map.get(profile, :manual_patch_disposition)
    owner_ref = Map.get(profile, :owner_approval_ref_when_manual_patch)

    cond do
      disposition not in @manual_patch_dispositions ->
        [{:invalid_manual_patch_disposition, disposition} | errors]

      disposition == @manual_patch_requires_owner_approval and
          owner_ref == @no_manual_patch_owner_ref ->
        [{:owner_approval_required_for_manual_patch, disposition} | errors]

      true ->
        errors
    end
  end

  defp require_projection_hash(errors, profile, true) do
    if sha256_ref?(Map.get(profile, :artifact_hash_manifest_ref)) do
      errors
    else
      [
        {:missing_projected_artifact_hash_manifest,
         Map.get(profile, :projection_branch_or_archive_ref)}
        | errors
      ]
    end
  end

  defp require_projection_hash(errors, _profile, false), do: errors

  defp projection_ref(root, projection_path) do
    relative_projection_path = Path.relative_to(Path.expand(projection_path, root), root)

    if File.dir?(Path.join(root, relative_projection_path)) do
      %{
        projection_branch_or_archive_ref: relative_projection_path,
        artifact_hash_manifest_ref: hash_manifest_ref(root, [relative_projection_path])
      }
    else
      %{
        projection_branch_or_archive_ref: @missing_projection_ref,
        artifact_hash_manifest_ref: @missing_projection_ref
      }
    end
  end

  defp source_root_ref_set(root, source_roots) do
    Enum.map(source_roots, fn source_root ->
      %{
        path: source_root,
        hash_ref: hash_manifest_ref(root, [source_root])
      }
    end)
  end

  defp artifact_roots do
    WeldContract.artifact()
    |> Keyword.fetch!(:roots)
  end

  defp contract_ref(root, path), do: "#{path}:#{file_hash_ref(root, path)}"

  defp git_ref(root) do
    case System.cmd("git", ["rev-parse", "--verify", "HEAD"], cd: root, stderr_to_stdout: true) do
      {sha, 0} -> String.trim(sha)
      {_error, _status} -> "unknown_git_ref"
    end
  end

  defp file_hash_ref(root, relative_path) do
    path = Path.join(root, relative_path)

    if File.regular?(path) do
      "sha256:#{path |> File.read!() |> sha256()}"
    else
      @missing_projection_ref
    end
  end

  defp hash_manifest_ref(root, paths) do
    entries =
      root
      |> source_files(paths)
      |> Enum.map(fn path ->
        relative_path = Path.relative_to(path, root)
        "#{relative_path}\0#{path |> File.read!() |> sha256()}"
      end)

    case entries do
      [] -> @missing_projection_ref
      _ -> "sha256:#{entries |> Enum.join("\n") |> sha256()}"
    end
  end

  defp source_files(root, paths) do
    paths
    |> Enum.flat_map(fn path ->
      absolute_path = Path.join(root, path)

      cond do
        File.regular?(absolute_path) -> [absolute_path]
        File.dir?(absolute_path) -> recursive_files(absolute_path)
        true -> []
      end
    end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp recursive_files(path) do
    path
    |> File.ls!()
    |> Enum.reject(&ignored_path?/1)
    |> Enum.flat_map(fn child ->
      child_path = Path.join(path, child)

      cond do
        File.regular?(child_path) -> [child_path]
        File.dir?(child_path) -> recursive_files(child_path)
        true -> []
      end
    end)
  end

  defp ignored_path?(path) do
    path in [".git", ".elixir_ls", "_build", "deps", "doc", "cover"]
  end

  defp sha256(binary) do
    :crypto.hash(:sha256, binary)
    |> Base.encode16(case: :lower)
  end

  defp sha256_ref?("sha256:" <> hash), do: byte_size(hash) == 64 and hex?(hash)
  defp sha256_ref?(_other), do: false

  defp ref_has_sha256?(ref) when is_binary(ref), do: String.contains?(ref, "sha256:")
  defp ref_has_sha256?(_other), do: false

  defp hex?(hash) do
    byte_size(hash) == 64 and
      hash
      |> :binary.bin_to_list()
      |> Enum.all?(fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end

  defp default_root, do: Path.expand("..", __DIR__)
end
