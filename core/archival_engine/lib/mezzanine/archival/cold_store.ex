defmodule Mezzanine.Archival.ColdStore do
  @moduledoc """
  Behaviour for archival cold-storage persistence keyed by `manifest_ref`.
  """

  @type write_result :: %{storage_uri: String.t(), checksum: String.t()}

  @callback write_bundle(String.t(), map(), keyword()) :: {:ok, write_result()} | {:error, term()}
  @callback read_bundle(String.t(), keyword()) :: {:ok, map()} | {:error, term()}

  @spec module() :: module()
  def module do
    Application.fetch_env!(:mezzanine_archival_engine, :cold_store)
    |> Keyword.fetch!(:module)
  end

  @spec write_bundle(String.t(), map(), keyword()) :: {:ok, write_result()} | {:error, term()}
  def write_bundle(manifest_ref, bundle, opts \\ [])
      when is_binary(manifest_ref) and is_map(bundle) do
    module().write_bundle(manifest_ref, bundle, opts)
  end

  @spec read_bundle(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def read_bundle(storage_uri, opts \\ []) when is_binary(storage_uri) do
    module().read_bundle(storage_uri, opts)
  end
end

defmodule Mezzanine.Archival.FileSystemColdStore do
  @moduledoc false

  @behaviour Mezzanine.Archival.ColdStore

  alias Mezzanine.Archival.BundleChecksum

  @impl true
  def write_bundle(manifest_ref, bundle, opts \\ []) do
    checksum = BundleChecksum.generate(bundle)

    with {:ok, encoded} <- Jason.encode(bundle, pretty: true),
         path <- bundle_path(manifest_ref, opts),
         :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(path, encoded),
         {:ok, readback} <- File.read(path),
         true <- readback == encoded,
         true <- Map.get(bundle, "checksum") == checksum do
      {:ok, %{storage_uri: path, checksum: checksum}}
    else
      false -> {:error, :cold_store_verification_failed}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def read_bundle(storage_uri, _opts \\ []) do
    with {:ok, encoded} <- File.read(storage_uri) do
      Jason.decode(encoded)
    end
  end

  defp bundle_path(manifest_ref, opts) do
    Path.join(root(opts), manifest_ref <> ".json")
  end

  defp root(opts) do
    case Keyword.get(opts, :root) do
      nil ->
        Application.fetch_env!(:mezzanine_archival_engine, :cold_store)
        |> Keyword.fetch!(:root)

      value ->
        value
    end
  end
end
