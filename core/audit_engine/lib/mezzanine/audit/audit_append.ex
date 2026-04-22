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
  SET payload = jsonb_set(
        COALESCE(audit_facts.payload, '{}'::jsonb),
        '{audit_observability_counts}',
        COALESCE(audit_facts.payload->'audit_observability_counts', '{}'::jsonb) ||
          jsonb_build_object(
            'count_ref', 'mezzanine.audit_append.observability_counts.v1',
            'admitted_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,admitted_count}')::integer, 0) + 1,
            'deduped_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,deduped_count}')::integer, 0) + 1,
            'aggregated_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,aggregated_count}')::integer, 0),
            'dropped_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,dropped_count}')::integer, 0),
            'truncated_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,truncated_count}')::integer, 0),
            'hashed_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,hashed_count}')::integer, 0) + $14::integer,
            'spilled_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,spilled_count}')::integer, 0),
            'sampled_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,sampled_count}')::integer, 0),
            'rejected_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,rejected_count}')::integer, 0),
            'overflow_count',
              COALESCE((audit_facts.payload #>> '{audit_observability_counts,overflow_count}')::integer, 0)
          ),
        true
      ),
      updated_at = $13
  RETURNING id, idempotency_key
  """

  @insert_new_audit_fact_sql """
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
  ON CONFLICT (installation_id, idempotency_key) DO NOTHING
  RETURNING id, idempotency_key
  """

  @aggregate_repeated_audit_fact_sql """
  WITH target AS (
    SELECT
      id,
      COALESCE((payload #>> '{audit_amplification_guard,suppressed_count}')::integer, 0) + 1
        AS next_suppressed_count
    FROM audit_facts
    WHERE installation_id = $1
      AND idempotency_key = $2
    FOR UPDATE
  )
  UPDATE audit_facts AS facts
  SET payload =
        jsonb_set(
          jsonb_set(
            jsonb_set(
              jsonb_set(
                COALESCE(facts.payload, '{}'::jsonb),
                '{audit_amplification_guard,suppressed_count}',
                to_jsonb(target.next_suppressed_count),
                true
              ),
              '{audit_amplification_guard,last_seen_at}',
              to_jsonb($3::text),
              true
            ),
            '{audit_aggregation}',
            jsonb_build_object(
              'aggregate_counter_ref', $4::text,
              'overflow_counter_ref', $5::text,
              'safe_action', $6::text,
              'suppressed_count', target.next_suppressed_count,
              'last_suppressed_at', $3::text
            ),
            true
          ),
          '{audit_observability_counts}',
          COALESCE(facts.payload->'audit_observability_counts', '{}'::jsonb) ||
            jsonb_build_object(
              'count_ref', 'mezzanine.audit_append.observability_counts.v1',
              'admitted_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,admitted_count}')::integer, 0) + 1,
              'deduped_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,deduped_count}')::integer, 0),
              'aggregated_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,aggregated_count}')::integer, 0) + 1,
              'dropped_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,dropped_count}')::integer, 0),
              'truncated_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,truncated_count}')::integer, 0),
              'hashed_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,hashed_count}')::integer, 0) + $8::integer,
              'spilled_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,spilled_count}')::integer, 0),
              'sampled_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,sampled_count}')::integer, 0),
              'rejected_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,rejected_count}')::integer, 0),
              'overflow_count',
                COALESCE((facts.payload #>> '{audit_observability_counts,overflow_count}')::integer, 0)
            ),
          true
        ),
      updated_at = $7
  FROM target
  WHERE facts.id = target.id
  RETURNING facts.id, facts.idempotency_key
  """

  @required_fields [:installation_id, :trace_id, :fact_kind, :actor_ref, :occurred_at]
  @amplification_guard_key "audit_amplification_guard"
  @observability_counts_key "audit_observability_counts"
  @observability_counts_ref "mezzanine.audit_append.observability_counts.v1"
  @guard_ref "mezzanine.audit_amplification_guard.v1"
  @repeat_aggregation_ref "mezzanine.audit_repeat_aggregation.v1"
  @overflow_counter_ref "mezzanine.audit_overflow.count"
  @guard_window_ms 60_000
  @max_events_per_key_per_window 1
  @unavailable_guard_safe_action "reject_audit_append"
  @required_guard_fields [
    "admission_key",
    "window_ms",
    "max_events_per_key_per_window",
    "aggregate_counter_ref",
    "suppressed_count",
    "first_seen_at",
    "last_seen_at",
    "overflow_counter_ref",
    "unavailable_guard_safe_action"
  ]
  @required_admission_key_fields [
    "tenant_or_partition",
    "owner_package",
    "source_boundary",
    "event_name",
    "error_class",
    "safe_action",
    "canonical_idempotency_key_or_payload_hash"
  ]
  @observability_count_fields [
    "admitted_count",
    "deduped_count",
    "aggregated_count",
    "dropped_count",
    "truncated_count",
    "hashed_count",
    "spilled_count",
    "sampled_count",
    "rejected_count",
    "overflow_count"
  ]
  @failure_fact_markers [
    "failed",
    "failure",
    "fail_closed",
    "context_budget",
    "semantic_failure",
    "actor_evidence",
    "decision_evidence",
    "rejected",
    "rejection",
    "error",
    "mismatch",
    "overflow",
    "export_overflow",
    "replay",
    "revocation",
    "signature",
    "trust_root",
    "version_skew",
    "tenant_authority",
    "tenant",
    "authority",
    "artifact",
    "schema_hash",
    "webhook"
  ]
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

    with :ok <- ensure_required(attrs),
         :ok <- ensure_amplification_guard(attrs) do
      idempotency_key = append_idempotency_key(attrs)
      repo = Keyword.get(opts, :repo, Repo)
      hash_count = append_hash_count(attrs)
      attrs = put_observability_counts(attrs, hash_count)

      attrs
      |> audit_fact_params(idempotency_key)
      |> append_or_aggregate_fact(repo, attrs, idempotency_key, hash_count)
    end
  end

  defp audit_fact_params(attrs, idempotency_key) do
    [
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
  end

  defp append_or_aggregate_fact(params, repo, attrs, idempotency_key, hash_count) do
    if aggregate_repeated_failure?(attrs) do
      case SQL.query(repo, @insert_new_audit_fact_sql, params) do
        {:ok, %{rows: [[audit_fact_id, returned_idempotency_key]]}} ->
          audit_result(audit_fact_id, returned_idempotency_key || idempotency_key)

        {:ok, %{rows: []}} ->
          aggregate_repeated_fact(repo, attrs, idempotency_key, hash_count)

        {:error, error} ->
          {:error, error}
      end
    else
      standard_params = [
        DateTime.utc_now() |> DateTime.truncate(:microsecond),
        hash_count
      ]

      case SQL.query(repo, @insert_audit_fact_sql, params ++ standard_params) do
        {:ok, %{rows: [[audit_fact_id, returned_idempotency_key]]}} ->
          audit_result(audit_fact_id, returned_idempotency_key || idempotency_key)

        {:error, error} ->
          {:error, error}
      end
    end
  end

  defp aggregate_repeated_fact(repo, attrs, idempotency_key, hash_count) do
    guard = attrs |> map_value(:payload) |> guard_from_payload() |> stringify_keys()
    admission_key = guard |> Map.get("admission_key", %{}) |> stringify_keys()

    params = [
      string_value(attrs, :installation_id),
      idempotency_key,
      Map.get(guard, "last_seen_at", seen_at(occurred_at(attrs))),
      Map.get(guard, "aggregate_counter_ref"),
      Map.get(guard, "overflow_counter_ref"),
      Map.get(admission_key, "safe_action", "aggregate_repeated_audit_fact"),
      DateTime.utc_now() |> DateTime.truncate(:microsecond),
      hash_count
    ]

    case SQL.query(repo, @aggregate_repeated_audit_fact_sql, params) do
      {:ok, %{rows: [[audit_fact_id, returned_idempotency_key]]}} ->
        audit_result(audit_fact_id, returned_idempotency_key || idempotency_key)

      {:error, error} ->
        {:error, error}
    end
  end

  defp audit_result(audit_fact_id, idempotency_key) do
    {:ok,
     %{
       audit_fact_id: normalize_uuid(audit_fact_id),
       idempotency_key: idempotency_key
     }}
  end

  defp put_observability_counts(attrs, hash_count) do
    payload = map_value(attrs, :payload) || %{}

    Map.put(
      attrs,
      :payload,
      Map.put(payload, @observability_counts_key, initial_observability_counts(hash_count))
    )
  end

  defp initial_observability_counts(hash_count) do
    @observability_count_fields
    |> Map.new(&{&1, 0})
    |> Map.merge(%{
      "count_ref" => @observability_counts_ref,
      "admitted_count" => 1,
      "hashed_count" => hash_count
    })
  end

  @spec put_amplification_guard(map() | keyword(), keyword()) :: map()
  def put_amplification_guard(attrs, opts \\ []) when is_map(attrs) or is_list(attrs) do
    attrs =
      attrs
      |> normalize_attrs()
      |> Map.put_new(:occurred_at, DateTime.utc_now() |> DateTime.truncate(:microsecond))

    if amplification_guard_required?(attrs) do
      payload = map_value(attrs, :payload) || %{}

      Map.put(
        attrs,
        :payload,
        Map.put_new(payload, @amplification_guard_key, build_amplification_guard(attrs, opts))
      )
    else
      attrs
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

  defp ensure_amplification_guard(attrs) do
    if amplification_guard_required?(attrs) do
      attrs
      |> map_value(:payload)
      |> guard_from_payload()
      |> validate_amplification_guard()
    else
      :ok
    end
  end

  defp guard_from_payload(payload) when is_map(payload) do
    Map.get(payload, @amplification_guard_key)
  end

  defp guard_from_payload(_payload), do: nil

  defp validate_amplification_guard(nil) do
    {:error, {:missing_audit_amplification_guard, @required_guard_fields}}
  end

  defp validate_amplification_guard(guard) when is_map(guard) do
    guard = stringify_keys(guard)

    cond do
      missing_guard_fields(guard) != [] ->
        {:error, {:missing_audit_amplification_guard_fields, missing_guard_fields(guard)}}

      not valid_positive_integer?(Map.get(guard, "window_ms")) ->
        {:error, {:invalid_audit_amplification_guard, :window_ms}}

      not valid_positive_integer?(Map.get(guard, "max_events_per_key_per_window")) ->
        {:error, {:invalid_audit_amplification_guard, :max_events_per_key_per_window}}

      not valid_non_negative_integer?(Map.get(guard, "suppressed_count")) ->
        {:error, {:invalid_audit_amplification_guard, :suppressed_count}}

      not non_empty_string?(Map.get(guard, "first_seen_at")) ->
        {:error, {:invalid_audit_amplification_guard, :first_seen_at}}

      not non_empty_string?(Map.get(guard, "last_seen_at")) ->
        {:error, {:invalid_audit_amplification_guard, :last_seen_at}}

      Map.get(guard, "unavailable_guard_safe_action") != @unavailable_guard_safe_action ->
        {:error, {:invalid_audit_amplification_guard, :unavailable_guard_safe_action}}

      true ->
        validate_admission_key(Map.get(guard, "admission_key"))
    end
  end

  defp validate_amplification_guard(_guard) do
    {:error, {:invalid_audit_amplification_guard, :not_a_map}}
  end

  defp validate_admission_key(admission_key) when is_map(admission_key) do
    admission_key = stringify_keys(admission_key)
    missing = missing_admission_key_fields(admission_key)

    cond do
      missing != [] ->
        {:error, {:missing_audit_admission_key_fields, missing}}

      Enum.all?(@required_admission_key_fields, &non_empty_string?(Map.get(admission_key, &1))) ->
        :ok

      true ->
        {:error, {:invalid_audit_admission_key, @required_admission_key_fields}}
    end
  end

  defp validate_admission_key(_admission_key) do
    {:error, {:invalid_audit_amplification_guard, :admission_key}}
  end

  defp append_idempotency_key(attrs) do
    if aggregate_repeated_failure?(attrs) do
      guard = attrs |> map_value(:payload) |> guard_from_payload() |> stringify_keys()

      "audit-aggregate:" <>
        digest(%{
          "admission_key" => Map.get(guard, "admission_key"),
          "aggregate_counter_ref" => Map.get(guard, "aggregate_counter_ref"),
          "window_ms" => Map.get(guard, "window_ms"),
          "window_started_at" => aggregate_window_started_at(attrs, guard)
        })
    else
      idempotency_key(attrs)
    end
  end

  defp aggregate_repeated_failure?(attrs), do: amplification_guard_required?(attrs)

  defp append_hash_count(attrs) do
    cond do
      aggregate_repeated_failure?(attrs) -> 1
      present?(map_value(attrs, :idempotency_key)) -> 0
      true -> 1
    end
  end

  defp missing_guard_fields(guard),
    do: Enum.reject(@required_guard_fields, &Map.has_key?(guard, &1))

  defp missing_admission_key_fields(admission_key),
    do: Enum.reject(@required_admission_key_fields, &Map.has_key?(admission_key, &1))

  defp build_amplification_guard(attrs, opts) do
    occurred_at = occurred_at(attrs)
    window_ms = Keyword.get(opts, :window_ms, @guard_window_ms)
    seen_at = seen_at(occurred_at)

    %{
      "guard_ref" => Keyword.get(opts, :guard_ref, @guard_ref),
      "admission_key" => admission_key(attrs, opts),
      "window_ms" => window_ms,
      "window_started_at" =>
        Keyword.get(opts, :window_started_at, window_started_at(occurred_at, window_ms)),
      "max_events_per_key_per_window" =>
        Keyword.get(opts, :max_events_per_key_per_window, @max_events_per_key_per_window),
      "aggregate_counter_ref" =>
        Keyword.get(opts, :aggregate_counter_ref, @repeat_aggregation_ref),
      "suppressed_count" => Keyword.get(opts, :suppressed_count, 0),
      "first_seen_at" => Keyword.get(opts, :first_seen_at, seen_at),
      "last_seen_at" => Keyword.get(opts, :last_seen_at, seen_at),
      "overflow_counter_ref" => Keyword.get(opts, :overflow_counter_ref, @overflow_counter_ref),
      "unavailable_guard_safe_action" =>
        Keyword.get(opts, :unavailable_guard_safe_action, @unavailable_guard_safe_action)
    }
  end

  defp admission_key(attrs, opts) do
    payload = map_value(attrs, :payload) || %{}
    fact_kind = fact_kind(attrs)

    %{
      "tenant_or_partition" => string_value(attrs, :installation_id),
      "owner_package" => Keyword.get(opts, :owner_package, "core/audit_engine"),
      "source_boundary" => Keyword.get(opts, :source_boundary, "Mezzanine.Audit.AuditAppend"),
      "event_name" => fact_kind,
      "error_class" => error_class(fact_kind, payload),
      "safe_action" => Keyword.get(opts, :safe_action, "aggregate_repeated_audit_fact"),
      "canonical_idempotency_key_or_payload_hash" =>
        Keyword.get(opts, :canonical_idempotency_key_or_payload_hash, idempotency_key(attrs))
    }
  end

  defp amplification_guard_required?(attrs) do
    attrs
    |> failure_markers()
    |> Enum.any?(&failure_marker?/1)
  end

  defp failure_markers(attrs) do
    payload = map_value(attrs, :payload) || %{}

    [
      fact_kind(attrs),
      payload_value(payload, "classification"),
      payload_value(payload, "failure_kind"),
      payload_value(payload, "error_kind")
    ]
    |> Enum.reject(&is_nil/1)
    |> Enum.map(&to_string/1)
  end

  defp failure_marker?(marker) do
    marker = String.downcase(marker)
    Enum.any?(@failure_fact_markers, &String.contains?(marker, &1))
  end

  defp error_class(fact_kind, payload) do
    payload_value(payload, "classification") ||
      payload_value(payload, "failure_kind") ||
      payload_value(payload, "error_kind") ||
      fact_kind
  end

  defp aggregate_window_started_at(attrs, guard) do
    Map.get(guard, "window_started_at") ||
      window_started_at(occurred_at(attrs), Map.get(guard, "window_ms", @guard_window_ms))
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

  defp stringify_keys(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {to_string(key), nested} end)
  end

  defp map_value(attrs, key), do: Map.get(attrs, key) || Map.get(attrs, to_string(key))

  defp payload_value(payload, key) when is_map(payload) do
    Map.get(payload, key)
  end

  defp payload_value(_payload, _key), do: nil

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

  defp seen_at(%DateTime{} = datetime),
    do: datetime |> DateTime.truncate(:microsecond) |> DateTime.to_iso8601()

  defp seen_at(%NaiveDateTime{} = datetime), do: NaiveDateTime.to_iso8601(datetime)
  defp seen_at(value) when is_binary(value), do: value

  defp seen_at(_value),
    do: DateTime.utc_now() |> DateTime.truncate(:microsecond) |> DateTime.to_iso8601()

  defp window_started_at(%DateTime{} = datetime, window_ms) when is_integer(window_ms) do
    millisecond = DateTime.to_unix(datetime, :millisecond)

    (millisecond - rem(millisecond, window_ms))
    |> DateTime.from_unix!(:millisecond)
    |> DateTime.to_iso8601()
  end

  defp window_started_at(%NaiveDateTime{} = datetime, window_ms) do
    datetime
    |> DateTime.from_naive!("Etc/UTC")
    |> window_started_at(window_ms)
  end

  defp window_started_at(value, window_ms) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> window_started_at(datetime, window_ms)
      {:error, _reason} -> value
    end
  end

  defp window_started_at(_value, window_ms),
    do: window_started_at(DateTime.utc_now() |> DateTime.truncate(:microsecond), window_ms)

  defp valid_positive_integer?(value), do: is_integer(value) and value > 0
  defp valid_non_negative_integer?(value), do: is_integer(value) and value >= 0

  defp non_empty_string?(value) when is_binary(value), do: String.trim(value) != ""
  defp non_empty_string?(_value), do: false

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
