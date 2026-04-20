defmodule Mezzanine.Objects.SubjectPayloadSchema do
  @moduledoc """
  Source-owned payload schema binding for durable subject ingest.

  The object ledger is the last local stop before a subject payload becomes
  durable truth. It must not accept arbitrary maps without an explicit schema
  identity that matches the subject kind.
  """

  @type payload_schema :: %{required(String.t()) => :string | :integer | :boolean | :map | :list}
  @type schema_record :: %{
          required(:subject_kind) => String.t(),
          required(:schema_ref) => String.t(),
          required(:schema_version) => pos_integer(),
          required(:payload_schema) => payload_schema()
        }

  @linear_coding_ticket_schema %{
    subject_kind: "linear_coding_ticket",
    schema_ref: "mezzanine.subject.linear_coding_ticket.payload.v1",
    schema_version: 1,
    payload_schema: %{
      "identifier" => :string,
      "source_kind" => :string,
      "title" => :string
    }
  }

  @schemas [@linear_coding_ticket_schema]
  @schema_index Map.new(@schemas, fn schema ->
                  {{schema.subject_kind, schema.schema_ref, schema.schema_version}, schema}
                end)
  @default_schema_by_subject Map.new(@schemas, fn schema -> {schema.subject_kind, schema} end)
  @current_schema_by_subject_ref Map.new(@schemas, fn schema ->
                                   {{schema.subject_kind, schema.schema_ref}, schema}
                                 end)

  @spec default_schema_ref!(String.t()) :: String.t()
  def default_schema_ref!(subject_kind) when is_binary(subject_kind) do
    subject_kind
    |> default_schema!()
    |> Map.fetch!(:schema_ref)
  end

  @spec default_schema_version!(String.t()) :: pos_integer()
  def default_schema_version!(subject_kind) when is_binary(subject_kind) do
    subject_kind
    |> default_schema!()
    |> Map.fetch!(:schema_version)
  end

  @spec schema_hash!(String.t(), String.t(), pos_integer()) :: String.t()
  def schema_hash!(subject_kind, schema_ref, schema_version) do
    subject_kind
    |> fetch!(schema_ref, schema_version)
    |> schema_hash()
  end

  @spec quarantine_ref(atom(), term(), term(), term()) :: String.t()
  def quarantine_ref(reason, subject_kind, schema_ref, schema_version) when is_atom(reason) do
    encoded =
      Enum.map_join([reason, subject_kind, schema_ref, schema_version], "\n", &inspect/1)

    "subject-payload-schema-quarantine:" <>
      Base.encode16(:crypto.hash(:sha256, encoded), case: :lower)
  end

  @spec validate_ingest(map()) ::
          {:ok, %{payload: map(), schema: schema_record(), schema_hash: String.t()}}
          | {:error, term()}
  def validate_ingest(attrs) when is_map(attrs) do
    with {:ok, subject_kind} <- fetch_required_string(attrs, :subject_kind),
         {:ok, schema_ref} <- fetch_required_string(attrs, :schema_ref),
         {:ok, schema_version} <- fetch_required_version(attrs, :schema_version),
         {:ok, schema} <- fetch(subject_kind, schema_ref, schema_version),
         {:ok, payload} <- normalize_payload(value(attrs, :payload) || %{}),
         :ok <- validate_payload(schema.payload_schema, payload) do
      {:ok, %{payload: payload, schema: schema, schema_hash: schema_hash(schema)}}
    end
  end

  def validate_ingest(_attrs), do: {:error, :invalid_ingest_attrs}

  @spec fetch(String.t(), String.t(), pos_integer()) :: {:ok, schema_record()} | {:error, term()}
  def fetch(subject_kind, schema_ref, schema_version)
      when is_binary(subject_kind) and is_binary(schema_ref) and is_integer(schema_version) do
    case Map.fetch(@schema_index, {subject_kind, schema_ref, schema_version}) do
      {:ok, schema} ->
        {:ok, schema}

      :error ->
        reject_unknown_schema(subject_kind, schema_ref, schema_version)
    end
  end

  def fetch(_subject_kind, _schema_ref, _schema_version),
    do: {:error, :invalid_subject_payload_schema_identity}

  @spec fetch!(String.t(), String.t(), pos_integer()) :: schema_record()
  def fetch!(subject_kind, schema_ref, schema_version) do
    case fetch(subject_kind, schema_ref, schema_version) do
      {:ok, schema} ->
        schema

      {:error, reason} ->
        raise ArgumentError, "unknown subject payload schema: #{inspect(reason)}"
    end
  end

  @spec schema_hash(schema_record()) :: String.t()
  def schema_hash(schema) when is_map(schema) do
    encoded =
      [
        schema.subject_kind,
        schema.schema_ref,
        Integer.to_string(schema.schema_version),
        schema.payload_schema
        |> Enum.sort_by(fn {field, _type} -> field end)
        |> Enum.map_join("|", fn {field, type} -> "#{field}:#{type}" end)
      ]
      |> Enum.join("\n")

    "sha256:" <> Base.encode16(:crypto.hash(:sha256, encoded), case: :lower)
  end

  defp default_schema!(subject_kind) do
    Map.fetch!(@default_schema_by_subject, subject_kind)
  end

  defp fetch_required_string(attrs, field) do
    case value(attrs, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:missing_subject_payload_schema_field, field}}
    end
  end

  defp fetch_required_version(attrs, field) do
    case value(attrs, field) do
      nil -> {:error, {:missing_subject_payload_schema_field, field}}
      value when is_integer(value) -> {:ok, value}
      _other -> {:error, {:invalid_subject_payload_schema_field, field}}
    end
  end

  defp reject_unknown_schema(subject_kind, schema_ref, schema_version) do
    cond do
      not Map.has_key?(@default_schema_by_subject, subject_kind) ->
        {:error,
         {:unknown_subject_payload_schema_subject_kind,
          %{
            subject_kind: subject_kind,
            schema_ref: schema_ref,
            schema_version: schema_version,
            quarantine_ref:
              quarantine_ref(:unknown_subject_kind, subject_kind, schema_ref, schema_version)
          }}}

      not Map.has_key?(@current_schema_by_subject_ref, {subject_kind, schema_ref}) ->
        {:error,
         {:unknown_subject_payload_schema_ref,
          %{
            subject_kind: subject_kind,
            schema_ref: schema_ref,
            schema_version: schema_version,
            quarantine_ref:
              quarantine_ref(:unknown_schema_ref, subject_kind, schema_ref, schema_version)
          }}}

      stale_schema_version?(subject_kind, schema_ref, schema_version) ->
        current_schema = Map.fetch!(@current_schema_by_subject_ref, {subject_kind, schema_ref})

        {:error,
         {:stale_subject_payload_schema_version,
          %{
            subject_kind: subject_kind,
            schema_ref: schema_ref,
            schema_version: schema_version,
            current_schema_version: current_schema.schema_version,
            quarantine_ref:
              quarantine_ref(:stale_schema_version, subject_kind, schema_ref, schema_version)
          }}}

      true ->
        current_schema = Map.fetch!(@current_schema_by_subject_ref, {subject_kind, schema_ref})

        {:error,
         {:unknown_subject_payload_schema_version,
          %{
            subject_kind: subject_kind,
            schema_ref: schema_ref,
            schema_version: schema_version,
            current_schema_version: current_schema.schema_version,
            quarantine_ref:
              quarantine_ref(:unknown_schema_version, subject_kind, schema_ref, schema_version)
          }}}
    end
  end

  defp stale_schema_version?(subject_kind, schema_ref, schema_version) do
    current_schema = Map.fetch!(@current_schema_by_subject_ref, {subject_kind, schema_ref})

    schema_version < current_schema.schema_version
  end

  defp normalize_payload(payload) when is_map(payload) do
    Enum.reduce_while(payload, {:ok, %{}}, fn {field, payload_value}, {:ok, acc} ->
      case normalize_field(field) do
        {:ok, normalized_field} -> {:cont, {:ok, Map.put(acc, normalized_field, payload_value)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp normalize_payload(_payload), do: {:error, :invalid_subject_payload}

  defp validate_payload(payload_schema, payload) do
    schema_fields = Map.keys(payload_schema) |> MapSet.new()
    payload_fields = Map.keys(payload) |> MapSet.new()

    case payload_fields |> MapSet.difference(schema_fields) |> MapSet.to_list() |> Enum.sort() do
      [] -> validate_payload_types(payload_schema, payload)
      unknown_fields -> {:error, {:unknown_subject_payload_fields, unknown_fields}}
    end
  end

  defp validate_payload_types(payload_schema, payload) do
    Enum.reduce_while(payload, :ok, fn {field, payload_value}, :ok ->
      expected_type = Map.fetch!(payload_schema, field)

      if valid_type?(payload_value, expected_type) do
        {:cont, :ok}
      else
        {:halt, {:error, {:invalid_subject_payload_field, field, expected_type}}}
      end
    end)
  end

  defp valid_type?(value, :string), do: is_binary(value)
  defp valid_type?(value, :integer), do: is_integer(value)
  defp valid_type?(value, :boolean), do: is_boolean(value)
  defp valid_type?(value, :map), do: is_map(value)
  defp valid_type?(value, :list), do: is_list(value)

  defp normalize_field(field) when is_binary(field) and field != "", do: {:ok, field}
  defp normalize_field(field) when is_atom(field), do: {:ok, Atom.to_string(field)}
  defp normalize_field(field), do: {:error, {:invalid_subject_payload_field, field}}

  defp value(attrs, field), do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))
end
