defmodule Mezzanine.Audit.AuditAppend do
  @moduledoc """
  Audit-owned append command for durable audit facts.

  Callers that already hold a transaction can pass their repo through `:repo`
  so the audit append remains in the same commit boundary without re-owning
  `audit_facts` SQL in the aggregate or command module.
  """

  alias Ecto.Adapters.SQL
  alias Mezzanine.Audit.Repo

  @insert_audit_fact_sql """
  INSERT INTO audit_facts (
    id,
    installation_id,
    subject_id,
    execution_id,
    decision_id,
    evidence_id,
    trace_id,
    causation_id,
    fact_kind,
    actor_ref,
    payload,
    occurred_at,
    idempotency_key,
    inserted_at,
    updated_at
  )
  VALUES (
    gen_random_uuid(),
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    $12,
    $11,
    $11
  )
  ON CONFLICT (installation_id, idempotency_key) DO UPDATE
  SET idempotency_key = audit_facts.idempotency_key
  RETURNING id, idempotency_key
  """

  @required_fields [:installation_id, :trace_id, :fact_kind, :actor_ref, :occurred_at]
  @identity_fields [
    :installation_id,
    :subject_id,
    :execution_id,
    :decision_id,
    :evidence_id,
    :trace_id,
    :causation_id,
    :fact_kind,
    :payload
  ]

  @type append_result :: %{
          audit_fact_id: String.t(),
          idempotency_key: String.t()
        }

  @spec append_fact(map() | keyword()) :: {:ok, append_result()} | {:error, term()}
  def append_fact(attrs) when is_map(attrs) or is_list(attrs), do: append_fact(attrs, [])

  @spec append_fact(map() | keyword(), keyword()) :: {:ok, append_result()} | {:error, term()}
  def append_fact(attrs, opts) when is_map(attrs) or is_list(attrs) do
    attrs =
      attrs
      |> normalize_attrs()
      |> Map.put_new(:occurred_at, DateTime.utc_now() |> DateTime.truncate(:microsecond))

    with :ok <- ensure_required(attrs) do
      idempotency_key = idempotency_key(attrs)
      repo = Keyword.get(opts, :repo, Repo)

      params = [
        string_value(attrs, :installation_id),
        optional_string_value(attrs, :subject_id),
        optional_string_value(attrs, :execution_id),
        optional_string_value(attrs, :decision_id),
        optional_string_value(attrs, :evidence_id),
        string_value(attrs, :trace_id),
        optional_string_value(attrs, :causation_id),
        fact_kind(attrs),
        map_value(attrs, :actor_ref),
        map_value(attrs, :payload) || %{},
        occurred_at(attrs),
        idempotency_key
      ]

      case SQL.query(repo, @insert_audit_fact_sql, params) do
        {:ok, %{rows: [[audit_fact_id, returned_idempotency_key]]}} ->
          {:ok,
           %{
             audit_fact_id: normalize_uuid(audit_fact_id),
             idempotency_key: returned_idempotency_key || idempotency_key
           }}

        {:error, error} ->
          {:error, error}
      end
    end
  end

  @spec idempotency_key(map() | keyword()) :: String.t()
  def idempotency_key(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = normalize_attrs(attrs)

    case map_value(attrs, :idempotency_key) do
      value when is_binary(value) and byte_size(value) > 0 ->
        value

      _other ->
        "audit-fact:" <> digest(identity_attrs(attrs))
    end
  end

  defp ensure_required(attrs) do
    missing =
      Enum.reject(@required_fields, fn field ->
        present?(map_value(attrs, field))
      end)

    case missing do
      [] -> :ok
      _missing -> {:error, {:missing_audit_append_fields, missing}}
    end
  end

  defp present?(nil), do: false
  defp present?(value) when is_binary(value), do: byte_size(value) > 0
  defp present?(_value), do: true

  defp normalize_attrs(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize_attrs()

  defp normalize_attrs(attrs) when is_map(attrs) do
    Map.new(attrs, fn {key, value} -> {normalize_key(key), normalize_value(value)} end)
  end

  defp normalize_key(key) when is_atom(key) or is_binary(key), do: key

  defp normalize_value(nil), do: nil
  defp normalize_value(%DateTime{} = datetime), do: DateTime.truncate(datetime, :microsecond)
  defp normalize_value(%NaiveDateTime{} = datetime), do: datetime
  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_boolean(value), do: value
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp normalize_map(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {to_string(key), normalize_value(nested)} end)
  end

  defp map_value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, to_string(key))

  defp identity_attrs(attrs) do
    Map.new(@identity_fields, fn field -> {field, map_value(attrs, field)} end)
  end

  defp string_value(attrs, key), do: attrs |> map_value(key) |> to_string()

  defp optional_string_value(attrs, key) do
    case map_value(attrs, key) do
      nil -> nil
      value -> to_string(value)
    end
  end

  defp fact_kind(attrs), do: attrs |> map_value(:fact_kind) |> to_string()

  defp occurred_at(attrs) do
    case map_value(attrs, :occurred_at) do
      %DateTime{} = datetime -> DateTime.truncate(datetime, :microsecond)
      other -> other
    end
  end

  defp digest(value) do
    :sha256
    |> :crypto.hash(:erlang.term_to_binary(canonical(value)))
    |> Base.encode16(case: :lower)
  end

  defp canonical(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp canonical(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)

  defp canonical(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested} -> {to_string(key), canonical(nested)} end)
    |> Enum.sort_by(fn {key, _value} -> key end)
  end

  defp canonical(value) when is_list(value), do: Enum.map(value, &canonical/1)
  defp canonical(value), do: value

  defp normalize_uuid(uuid) when is_binary(uuid) and byte_size(uuid) == 16,
    do: Ecto.UUID.load!(uuid)

  defp normalize_uuid(uuid) when is_binary(uuid), do: uuid
  defp normalize_uuid(uuid), do: Ecto.UUID.load!(uuid)
end
