defmodule Mezzanine.WorkspaceEngine.PathSafety do
  @moduledoc """
  Local workspace path safety checks shared by allocation and cleanup code.
  """

  @spec slug(term()) :: String.t()
  def slug(value) do
    value
    |> to_string()
    |> safe_slug_chars()
    |> case do
      "" -> "unidentified"
      "." -> "unidentified"
      ".." -> "unidentified"
      slug -> slug
    end
  end

  defp safe_slug_chars(value) do
    value
    |> :binary.bin_to_list()
    |> Enum.map(fn byte ->
      if byte in ?A..?Z or byte in ?a..?z or byte in ?0..?9 or byte in [?., ?_, ?-] do
        byte
      else
        ?_
      end
    end)
    |> List.to_string()
  end

  @spec canonicalize(String.t()) :: String.t()
  def canonicalize(path) when is_binary(path), do: canonical_existing(path)

  @spec prepare_directory(String.t(), String.t()) :: :ok | {:error, atom()}
  def prepare_directory(root, path) when is_binary(root) and is_binary(path) do
    with :ok <- validate(root, path) do
      path = canonicalize(path)

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
    expanded_root = Path.expand(root)
    canonical_root = canonicalize(root)
    expanded_path = Path.expand(path)
    canonical_path = canonicalize(path)

    cond do
      canonical_path == canonical_root ->
        {:error, :workspace_is_root}

      not under_root?(canonical_root, canonical_path) and
          (under_root?(canonical_root, expanded_path) or under_root?(expanded_root, expanded_path)) ->
        {:error, :symlink_escape}

      not under_root?(canonical_root, canonical_path) ->
        {:error, :outside_workspace_root}

      true ->
        :ok
    end
  end

  @spec safety_hash(String.t(), String.t(), map()) :: String.t()
  def safety_hash(root, path, metadata \\ %{}) do
    ["workspace", canonicalize(root), canonicalize(path), metadata]
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
    |> then(&("sha256:" <> &1))
  end

  defp under_root?(root, path) do
    String.starts_with?(Path.expand(path), root <> "/")
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
