defmodule Mezzanine.ContentStore do
  @moduledoc "Pure content store metadata contract."

  alias Mezzanine.Substrate.Builder

  defmodule Entry do
    @moduledoc "Immutable content entry metadata and body holder."
    @enforce_keys [
      :content_ref,
      :owner_ref,
      :tenant_ref,
      :installation_ref,
      :schema_ref,
      :redaction_ref,
      :content_hash,
      :byte_size
    ]
    defstruct @enforce_keys ++ [body: nil, retention_refs: [], metadata: %{}]

    @type t :: %__MODULE__{}
  end

  @spec put(map(), map() | keyword()) :: {:ok, map(), Entry.t()} | {:error, term()}
  def put(store, attrs) when is_map(store) do
    with {:ok, attrs} <- Builder.normalize_attrs(attrs),
         :ok <- Builder.reject_forbidden_fields(attrs),
         {:ok, entry} <- build_entry(attrs),
         :ok <- validate_body_hash(entry) do
      {:ok, Map.put(store, entry.content_ref, entry), entry}
    end
  end

  @spec fetch(map(), String.t(), map()) :: {:ok, Entry.t()} | {:error, term()}
  def fetch(store, content_ref, context) when is_map(store) and is_binary(content_ref) do
    with {:ok, entry} <- Map.fetch(store, content_ref),
         :ok <- authorize(entry, context) do
      {:ok, entry}
    else
      :error -> {:error, :content_not_found}
      error -> error
    end
  end

  @spec delete(map(), String.t()) :: {:ok, map()} | {:error, term()}
  def delete(store, content_ref) when is_map(store) do
    with {:ok, entry} <- Map.fetch(store, content_ref),
         :ok <- deletable?(entry) do
      {:ok, Map.delete(store, content_ref)}
    else
      :error -> {:error, :content_not_found}
      error -> error
    end
  end

  defp build_entry(attrs) do
    required =
      Entry.__struct__()
      |> Map.keys()
      |> Enum.reject(&(&1 in [:__struct__, :body, :retention_refs, :metadata]))

    with :ok <- require_fields(attrs, required) do
      {:ok, struct(Entry, attrs)}
    end
  end

  defp require_fields(attrs, fields) do
    case Enum.find(fields, &(not present?(Map.get(attrs, &1)))) do
      nil -> :ok
      field -> {:error, {:missing_content_field, field}}
    end
  end

  defp validate_body_hash(%Entry{body: nil}), do: :ok

  defp validate_body_hash(%Entry{body: body, content_hash: "sha256:" <> expected}) do
    actual =
      :crypto.hash(:sha256, to_string(body))
      |> Base.encode16(case: :lower)

    if actual == expected, do: :ok, else: {:error, :content_hash_mismatch}
  end

  defp validate_body_hash(_entry), do: {:error, :invalid_content_hash}

  defp authorize(entry, context) do
    if entry.tenant_ref == Map.get(context, :tenant_ref) and
         entry.installation_ref == Map.get(context, :installation_ref) do
      :ok
    else
      {:error, :unauthorized_content_access}
    end
  end

  defp deletable?(%Entry{retention_refs: []}), do: :ok
  defp deletable?(%Entry{retention_refs: refs}), do: {:error, {:content_retained, refs}}

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value), do: not is_nil(value)
end
