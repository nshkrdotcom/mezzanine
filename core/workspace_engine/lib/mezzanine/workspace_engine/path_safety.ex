defmodule Mezzanine.WorkspaceEngine.PathSafety do
  @moduledoc """
  Local workspace path safety checks shared by allocation and cleanup code.
  """

  @spec slug(term()) :: String.t()
  def slug(value) do
    value
    |> to_string()
    |> String.replace(~r/[^A-Za-z0-9._-]/, "_")
    |> case do
      "" -> "unidentified"
      "." -> "unidentified"
      ".." -> "unidentified"
      slug -> slug
    end
  end

  @spec prepare_directory(String.t(), String.t()) :: :ok | {:error, atom()}
  def prepare_directory(root, path) when is_binary(root) and is_binary(path) do
    with :ok <- validate(root, path) do
      cond do
        File.dir?(path) ->
          :ok

        File.exists?(path) ->
          File.rm!(path)
          File.mkdir_p(path)

        true ->
          File.mkdir_p(path)
      end
    end
  end

  @spec validate(String.t(), String.t()) :: :ok | {:error, atom()}
  def validate(root, path) when is_binary(root) and is_binary(path) do
    root = canonical_root(root)
    path = Path.expand(path)

    cond do
      Path.expand(path) == root ->
        {:error, :workspace_is_root}

      not under_root?(root, path) ->
        {:error, :outside_workspace_root}

      symlink_escape?(root, path) ->
        {:error, :symlink_escape}

      true ->
        :ok
    end
  end

  @spec safety_hash(String.t(), String.t(), map()) :: String.t()
  def safety_hash(root, path, metadata \\ %{}) do
    ["workspace", canonical_root(root), Path.expand(path), metadata]
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> then(&("sha256:" <> &1))
  end

  defp canonical_root(root) do
    canonical_existing(root)
  end

  defp under_root?(root, path) do
    String.starts_with?(Path.expand(path), root <> "/")
  end

  defp symlink_escape?(root, path) do
    not under_root?(root, canonical_existing(path))
  end

  defp canonical_existing(path) do
    expanded = Path.expand(path)

    case Path.split(expanded) do
      [root | parts] -> resolve_parts(root, parts)
      [] -> expanded
    end
  end

  defp resolve_parts(current, []), do: current

  defp resolve_parts(current, [part | rest]) do
    candidate = Path.join(current, part)

    case File.lstat(candidate) do
      {:ok, %{type: :symlink}} ->
        case File.read_link(candidate) do
          {:ok, target} -> resolve_parts(Path.expand(target, current), rest)
          {:error, _reason} -> candidate
        end

      {:ok, _stat} ->
        resolve_parts(candidate, rest)

      {:error, _reason} ->
        Enum.reduce(rest, candidate, &Path.join(&2, &1))
    end
  end
end
