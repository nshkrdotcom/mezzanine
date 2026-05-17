defmodule Mezzanine.Substrate.PayloadEnvelope do
  @moduledoc "Payload storage envelope."

  alias Mezzanine.Substrate.Builder

  @enforce_keys [:payload_ref, :storage_mode, :schema_ref, :redaction_ref]
  defstruct @enforce_keys ++
              [
                :data,
                :content_ref,
                :content_hash,
                :byte_size,
                :store_ref,
                :stream_ref,
                retention_refs: [],
                metadata: %{}
              ]

  @type t :: %__MODULE__{}

  @spec fields() :: [atom()]
  def fields,
    do:
      @enforce_keys ++
        [
          :data,
          :content_ref,
          :content_hash,
          :byte_size,
          :store_ref,
          :stream_ref,
          :retention_refs,
          :metadata
        ]

  @spec required_fields() :: [atom()]
  def required_fields, do: @enforce_keys

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs),
    do: Builder.build(__MODULE__, attrs, @enforce_keys, validate: [&validate_storage/1])

  defp validate_storage(%{storage_mode: :inline} = attrs) do
    if Map.has_key?(attrs, :data), do: :ok, else: {:error, {:missing_content_field, :data}}
  end

  defp validate_storage(%{storage_mode: :content_addressed} = attrs) do
    require_content_fields(attrs, [:content_ref, :content_hash, :byte_size, :store_ref])
  end

  defp validate_storage(%{storage_mode: :stream} = attrs) do
    require_content_fields(attrs, [:stream_ref, :store_ref])
  end

  defp validate_storage(%{storage_mode: mode}), do: {:error, {:unsupported_storage_mode, mode}}

  defp require_content_fields(attrs, fields) do
    case Enum.find(fields, &(not Map.has_key?(attrs, &1))) do
      nil -> validate_content_hash(attrs)
      field -> {:error, {:missing_content_field, field}}
    end
  end

  defp validate_content_hash(%{content_hash: hash}) do
    if sha256_hash?(hash), do: :ok, else: {:error, :invalid_content_hash}
  end

  defp validate_content_hash(_attrs), do: :ok

  defp sha256_hash?("sha256:" <> hex) when byte_size(hex) == 64 do
    hex
    |> String.to_charlist()
    |> Enum.all?(&hex_char?/1)
  end

  defp sha256_hash?(_hash), do: false
  defp hex_char?(char), do: char in ?0..?9 or char in ?a..?f
end

defmodule Mezzanine.Substrate.ResultEnvelope do
  @moduledoc "Result storage envelope."

  alias Mezzanine.Substrate.Builder
  alias Mezzanine.Substrate.PayloadEnvelope

  @enforce_keys [:result_ref, :storage_mode, :schema_ref, :redaction_ref]
  defstruct @enforce_keys ++
              [
                :data,
                :content_ref,
                :content_hash,
                :byte_size,
                :store_ref,
                :stream_ref,
                retention_refs: [],
                metadata: %{}
              ]

  @type t :: %__MODULE__{}

  @spec fields() :: [atom()]
  def fields,
    do:
      @enforce_keys ++
        [
          :data,
          :content_ref,
          :content_hash,
          :byte_size,
          :store_ref,
          :stream_ref,
          :retention_refs,
          :metadata
        ]

  @spec required_fields() :: [atom()]
  def required_fields, do: @enforce_keys

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) do
    Builder.build(__MODULE__, attrs, @enforce_keys, validate: [&validate_as_payload/1])
  end

  defp validate_as_payload(attrs) do
    attrs
    |> Map.put(:payload_ref, Map.fetch!(attrs, :result_ref))
    |> PayloadEnvelope.new()
    |> case do
      {:ok, _payload} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
