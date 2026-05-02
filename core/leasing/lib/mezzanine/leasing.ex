defmodule Mezzanine.Leasing do
  @moduledoc """
  Lease issuance, authorization, and invalidation for direct lower reads and
  stream attachment.
  """

  import Ecto.Query

  alias Mezzanine.LeaseInvalidation
  alias Mezzanine.Leasing.AuthorizationScope
  alias Mezzanine.ReadLease
  alias Mezzanine.StreamAttachLease
  alias Mezzanine.Telemetry

  @invalidation_lock_sql "SELECT pg_advisory_xact_lock(hashtext('lease_invalidations.write'))"
  @invalidation_result_columns %{
    "activation_epoch" => :activation_epoch,
    "cache_invalidation_ref" => :cache_invalidation_ref,
    "execution_id" => :execution_id,
    "id" => :id,
    "inserted_at" => :inserted_at,
    "installation_id" => :installation_id,
    "installation_revision" => :installation_revision,
    "invalidated_at" => :invalidated_at,
    "lease_epoch" => :lease_epoch,
    "lease_id" => :lease_id,
    "lease_kind" => :lease_kind,
    "reason" => :reason,
    "revocation_ref" => :revocation_ref,
    "sequence_number" => :sequence_number,
    "subject_id" => :subject_id,
    "tenant_id" => :tenant_id,
    "trace_id" => :trace_id
  }

  @type repo_option :: {:repo, module()}
  @type connection_option :: {:connection, pid()}
  @type trace_id_option :: {:trace_id, String.t()}
  @type limit_option :: {:limit, pos_integer()}
  @type lease_id_option :: {:lease_id, Ecto.UUID.t()}
  @type lease_kind_option :: {:lease_kind, :read | :stream | String.t()}
  @type option ::
          repo_option
          | connection_option
          | trace_id_option
          | limit_option
          | lease_id_option
          | lease_kind_option
          | {:now, DateTime.t()}
          | {:ttl_ms, pos_integer()}

  @spec issue_read_lease(map() | keyword(), [option()]) ::
          {:ok, ReadLease.t()} | {:error, term()}
  def issue_read_lease(attrs, opts \\ []) do
    attrs = normalize_attrs(attrs)
    repo = repo(opts)
    now = now(opts)
    ttl_ms = Keyword.get(opts, :ttl_ms, default_read_ttl_ms())
    token = generate_token()

    insert_lease(
      repo,
      ReadLease,
      %{
        lease_id: Ecto.UUID.generate(),
        trace_id: fetch_required!(attrs, :trace_id),
        tenant_id: fetch_required!(attrs, :tenant_id),
        installation_id: fetch_required!(attrs, :installation_id),
        installation_revision: fetch_required!(attrs, :installation_revision),
        activation_epoch: fetch_required!(attrs, :activation_epoch),
        lease_epoch: fetch_required!(attrs, :lease_epoch),
        subject_id: Map.get(attrs, :subject_id),
        execution_id: Map.get(attrs, :execution_id),
        lineage_anchor: Map.get(attrs, :lineage_anchor, %{}),
        allowed_family: fetch_required!(attrs, :allowed_family),
        allowed_operations:
          attrs
          |> Map.get(:allowed_operations, [])
          |> normalize_operations(),
        scope: Map.get(attrs, :scope, %{}),
        lease_token_digest: digest_token(token),
        lease_token: token,
        expires_at: Map.get(attrs, :expires_at, DateTime.add(now, ttl_ms, :millisecond)),
        issued_invalidation_cursor: current_invalidation_cursor(repo),
        invalidation_channel:
          invalidation_channel("read", Map.get(attrs, :allowed_family), Map.get(attrs, :trace_id))
      }
    )
  end

  @spec issue_stream_attach_lease(map() | keyword(), [option()]) ::
          {:ok, StreamAttachLease.t()} | {:error, term()}
  def issue_stream_attach_lease(attrs, opts \\ []) do
    attrs = normalize_attrs(attrs)
    repo = repo(opts)
    now = now(opts)
    ttl_ms = Keyword.get(opts, :ttl_ms, default_stream_ttl_ms())
    token = generate_token()
    cursor = current_invalidation_cursor(repo)

    insert_lease(
      repo,
      StreamAttachLease,
      %{
        lease_id: Ecto.UUID.generate(),
        trace_id: fetch_required!(attrs, :trace_id),
        tenant_id: fetch_required!(attrs, :tenant_id),
        installation_id: fetch_required!(attrs, :installation_id),
        installation_revision: fetch_required!(attrs, :installation_revision),
        activation_epoch: fetch_required!(attrs, :activation_epoch),
        lease_epoch: fetch_required!(attrs, :lease_epoch),
        subject_id: Map.get(attrs, :subject_id),
        execution_id: Map.get(attrs, :execution_id),
        lineage_anchor: Map.get(attrs, :lineage_anchor, %{}),
        allowed_family: fetch_required!(attrs, :allowed_family),
        scope: Map.get(attrs, :scope, %{}),
        attach_token_digest: digest_token(token),
        attach_token: token,
        expires_at: Map.get(attrs, :expires_at, DateTime.add(now, ttl_ms, :millisecond)),
        issued_invalidation_cursor: cursor,
        last_invalidation_cursor: cursor,
        invalidation_channel:
          invalidation_channel(
            "stream",
            Map.get(attrs, :allowed_family),
            Map.get(attrs, :trace_id)
          )
      }
    )
  end

  @spec authorize_read(
          AuthorizationScope.t() | map() | keyword(),
          Ecto.UUID.t(),
          String.t(),
          atom() | String.t(),
          [option()]
        ) ::
          {:ok, ReadLease.t()} | {:error, term()}
  def authorize_read(scope, lease_id, token, operation, opts \\ [])
      when is_binary(lease_id) and is_binary(token) do
    with {:ok, %AuthorizationScope{} = scope} <- AuthorizationScope.new(scope),
         {:ok, %ReadLease{} = lease} <- fetch_lease(ReadLease, lease_id, opts),
         :ok <- ensure_authorized_scope(lease, scope),
         :ok <- verify_token(lease.lease_token_digest, token),
         :ok <- ensure_not_expired(lease.expires_at, opts),
         :ok <- ensure_read_operation(lease.allowed_operations, operation),
         :ok <-
           ensure_not_invalidated(lease.lease_id, "read", lease.issued_invalidation_cursor, opts) do
      {:ok, lease}
    end
  end

  @spec authorize_stream_attach(
          AuthorizationScope.t() | map() | keyword(),
          Ecto.UUID.t(),
          String.t(),
          [option()]
        ) ::
          {:ok, StreamAttachLease.t()} | {:error, term()}
  def authorize_stream_attach(scope, lease_id, token, opts \\ [])
      when is_binary(lease_id) and is_binary(token) do
    with {:ok, %AuthorizationScope{} = scope} <- AuthorizationScope.new(scope),
         {:ok, %StreamAttachLease{} = lease} <- fetch_lease(StreamAttachLease, lease_id, opts),
         :ok <- ensure_authorized_scope(lease, scope),
         :ok <- verify_token(lease.attach_token_digest, token),
         :ok <- ensure_not_expired(lease.expires_at, opts),
         cursor <- lease.issued_invalidation_cursor || 0,
         :ok <- ensure_not_invalidated(lease.lease_id, "stream", cursor, opts) do
      {:ok, lease}
    end
  end

  @spec advance_stream_cursor(Ecto.UUID.t(), non_neg_integer(), [option()]) ::
          :ok | {:error, term()}
  def advance_stream_cursor(lease_id, sequence_number, opts \\ [])
      when is_binary(lease_id) and is_integer(sequence_number) and sequence_number >= 0 do
    case Keyword.get(opts, :connection) do
      nil ->
        {count, _rows} =
          repo(opts).update_all(
            from(lease in StreamAttachLease,
              where: lease.lease_id == ^lease_id,
              where:
                is_nil(lease.last_invalidation_cursor) or
                  lease.last_invalidation_cursor < ^sequence_number
            ),
            set: [last_invalidation_cursor: sequence_number, updated_at: now(opts)]
          )

        if count in [0, 1], do: :ok, else: {:error, :unexpected_cursor_update_count}

      connection ->
        advance_stream_cursor_via_connection(connection, lease_id, sequence_number, now(opts))
    end
  end

  @spec list_invalidations_after(non_neg_integer(), [option()]) :: {:ok, [LeaseInvalidation.t()]}
  def list_invalidations_after(cursor, opts \\ [])
      when is_integer(cursor) and cursor >= 0 do
    fetch_invalidations_after(query_target(opts), cursor, opts)
  end

  @spec invalidate_subject_leases(Ecto.UUID.t(), String.t(), [option()]) ::
          {:ok, [LeaseInvalidation.t()]} | {:error, term()}
  def invalidate_subject_leases(subject_id, reason, opts \\ [])
      when is_binary(subject_id) and is_binary(reason) do
    invalidate_matching_leases(
      [subject_id: subject_id],
      reason,
      opts
    )
  end

  @spec invalidate_execution_leases(Ecto.UUID.t(), String.t(), [option()]) ::
          {:ok, [LeaseInvalidation.t()]} | {:error, term()}
  def invalidate_execution_leases(execution_id, reason, opts \\ [])
      when is_binary(execution_id) and is_binary(reason) do
    invalidate_matching_leases(
      [execution_id: execution_id],
      reason,
      opts
    )
  end

  @spec invalidate_installation_leases(Ecto.UUID.t(), String.t(), [option()]) ::
          {:ok, [LeaseInvalidation.t()]} | {:error, term()}
  def invalidate_installation_leases(installation_id, reason, opts \\ [])
      when is_binary(installation_id) and is_binary(reason) do
    invalidate_matching_leases(
      [installation_id: installation_id],
      reason,
      opts
    )
  end

  @spec invalidate_tenant_leases(String.t(), String.t(), [option()]) ::
          {:ok, [LeaseInvalidation.t()]} | {:error, term()}
  def invalidate_tenant_leases(tenant_id, reason, opts \\ [])
      when is_binary(tenant_id) and is_binary(reason) do
    invalidate_matching_leases(
      [tenant_id: tenant_id],
      reason,
      opts
    )
  end

  @spec invalidate_stream_attach_lease(Ecto.UUID.t(), String.t(), [option()]) ::
          {:ok, [LeaseInvalidation.t()]} | {:error, term()}
  def invalidate_stream_attach_lease(lease_id, reason, opts \\ [])
      when is_binary(lease_id) and is_binary(reason) do
    invalidate_matching_leases(
      [stream_lease_id: lease_id],
      reason,
      opts
    )
  end

  defp insert_lease(repo, schema, attrs) do
    struct(schema)
    |> schema.changeset(attrs)
    |> repo.insert()
    |> case do
      {:ok, lease} ->
        emit_lease_issued(lease)
        {:ok, lease}

      error ->
        error
    end
  end

  defp fetch_lease(schema, lease_id, opts) do
    case repo(opts).get(schema, lease_id) do
      nil -> {:error, :lease_not_found}
      lease -> {:ok, lease}
    end
  end

  defp ensure_not_expired(expires_at, opts) do
    if DateTime.compare(expires_at, now(opts)) == :gt do
      :ok
    else
      {:error, :lease_expired}
    end
  end

  defp ensure_read_operation(allowed_operations, operation) do
    normalized = normalize_operation(operation)

    if normalized in allowed_operations do
      :ok
    else
      {:error, :unauthorized_operation}
    end
  end

  defp ensure_authorized_scope(lease, %AuthorizationScope{} = scope) do
    [
      {:tenant_mismatch, lease.tenant_id, scope.tenant_id, :required},
      {:installation_mismatch, lease.installation_id, scope.installation_id, :optional},
      {:installation_revision_mismatch, lease.installation_revision, scope.installation_revision,
       :required},
      {:activation_epoch_mismatch, lease.activation_epoch, scope.activation_epoch, :required},
      {:lease_epoch_mismatch, lease.lease_epoch, scope.lease_epoch, :required},
      {:trace_mismatch, lease.trace_id, scope.trace_id, :optional},
      {:subject_mismatch, lease.subject_id, scope.subject_id, :optional},
      {:execution_mismatch, lease.execution_id, scope.execution_id, :optional}
    ]
    |> Enum.reduce_while(:ok, fn check, :ok ->
      case ensure_scope_field(check) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp ensure_scope_field({reason, lease_value, scope_value, :required})
       when lease_value != scope_value,
       do: {:error, reason}

  defp ensure_scope_field({_reason, _lease_value, _scope_value, :required}), do: :ok
  defp ensure_scope_field({_reason, _lease_value, nil, :optional}), do: :ok
  defp ensure_scope_field({_reason, nil, _scope_value, :optional}), do: :ok

  defp ensure_scope_field({reason, lease_value, scope_value, :optional})
       when lease_value != scope_value,
       do: {:error, reason}

  defp ensure_scope_field({_reason, _lease_value, _scope_value, :optional}), do: :ok

  defp ensure_not_invalidated(lease_id, lease_kind, cursor, opts) do
    repo = repo(opts)

    case fetch_invalidations_after(repo, cursor,
           lease_id: lease_id,
           lease_kind: lease_kind,
           limit: 1
         ) do
      {:ok, []} ->
        :ok

      {:ok, [invalidation | _rest]} ->
        {:error, {:lease_invalidated, invalidation.reason, invalidation.sequence_number}}

      {:error, error} ->
        {:error, error}
    end
  end

  defp invalidate_matching_leases(filters, reason, opts) do
    repo = repo(opts)
    timestamp = now(opts)
    trace_id = Keyword.get(opts, :trace_id, "lease_invalidation")

    case repo.transaction(fn ->
           :ok = lock_invalidation_writes(repo)
           insert_or_rollback_invalidations(repo, filters, trace_id, reason, timestamp)
         end) do
      {:ok, rows} ->
        with :ok <- maybe_revoke_access_graph_edges(rows, opts) do
          Enum.each(rows, &emit_lease_invalidated/1)
          {:ok, rows}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp insert_or_rollback_invalidations(repo, filters, trace_id, reason, timestamp) do
    case build_invalidation_rows(filters, repo, trace_id, reason, timestamp) do
      {:ok, rows} ->
        rows

      {:error, insert_error} ->
        repo.rollback(insert_error)
    end
  end

  defp build_invalidation_rows(filters, repo, trace_id, reason, timestamp) do
    cursor = current_invalidation_cursor(repo)

    rows =
      filters
      |> candidate_invalidation_rows(repo, trace_id, reason, timestamp)
      |> with_sequence_numbers(cursor + 1)

    insert_invalidation_rows(repo, rows)
  end

  defp candidate_invalidation_rows(filters, repo, trace_id, reason, timestamp) do
    read_rows =
      ReadLease
      |> lease_filter_query(filters)
      |> repo.all()
      |> Enum.map(&invalidation_row(&1, "read", trace_id, reason, timestamp))

    stream_rows =
      StreamAttachLease
      |> lease_filter_query(filters)
      |> repo.all()
      |> Enum.map(&invalidation_row(&1, "stream", trace_id, reason, timestamp))

    read_rows ++ stream_rows
  end

  defp insert_invalidation_rows(_repo, []), do: {:ok, []}

  defp insert_invalidation_rows(repo, rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, acc} ->
      changeset = LeaseInvalidation.changeset(%LeaseInvalidation{}, row)

      case repo.insert(changeset) do
        {:ok, inserted} -> {:cont, {:ok, [inserted | acc]}}
        {:error, changeset} -> {:halt, {:error, changeset}}
      end
    end)
    |> case do
      {:ok, inserted} -> {:ok, Enum.reverse(inserted)}
      error -> error
    end
  end

  defp invalidation_row(lease, lease_kind, trace_id, reason, timestamp) do
    %{
      lease_id: lease.lease_id,
      lease_kind: lease_kind,
      tenant_id: lease.tenant_id,
      installation_id: lease.installation_id,
      installation_revision: lease.installation_revision,
      activation_epoch: lease.activation_epoch,
      lease_epoch: lease.lease_epoch,
      subject_id: lease.subject_id,
      execution_id: lease.execution_id,
      trace_id: trace_id,
      reason: reason,
      invalidated_at: timestamp,
      inserted_at: timestamp
    }
  end

  defp with_sequence_numbers(rows, start_at) do
    rows
    |> Enum.with_index(start_at)
    |> Enum.map(fn {row, sequence_number} ->
      row
      |> Map.put(:sequence_number, sequence_number)
      |> Map.put(
        :revocation_ref,
        "lease-revocation:#{row.lease_kind}:#{row.lease_id}:#{sequence_number}"
      )
      |> Map.put(
        :cache_invalidation_ref,
        "lease-cache-invalidation:#{row.lease_kind}:#{row.lease_id}:#{sequence_number}"
      )
    end)
  end

  defp lease_filter_query(queryable, filters) do
    Enum.reduce(filters, from(lease in queryable), fn
      {:subject_id, subject_id}, query ->
        where(query, [lease], lease.subject_id == ^subject_id)

      {:execution_id, execution_id}, query ->
        where(query, [lease], lease.execution_id == ^execution_id)

      {:installation_id, installation_id}, query ->
        where(query, [lease], lease.installation_id == ^installation_id)

      {:tenant_id, tenant_id}, query ->
        where(query, [lease], lease.tenant_id == ^tenant_id)

      {:stream_lease_id, lease_id}, query ->
        where(query, [lease], lease.lease_id == ^lease_id)
    end)
  end

  defp maybe_revoke_access_graph_edges([], _opts), do: :ok

  defp maybe_revoke_access_graph_edges(rows, opts) do
    rows
    |> access_graph_revocation_groups()
    |> revoke_access_graph_groups(rows, opts, access_graph_store(opts))
  end

  defp revoke_access_graph_groups(_groups, _rows, _opts, nil), do: :ok
  defp revoke_access_graph_groups([], _rows, _opts, _store), do: :ok

  defp revoke_access_graph_groups(groups, rows, opts, store) do
    revoking_authority_ref =
      Keyword.get(opts, :revoking_authority_ref) || default_revoking_authority_ref(rows)

    Enum.reduce_while(groups, :ok, fn {{tenant_id, subject_id}, grouped_rows}, :ok ->
      case store.revoke_subject_edges(
             tenant_id,
             subject_id,
             revoking_authority_ref,
             access_graph_revoke_opts(grouped_rows, opts)
           ) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp access_graph_revocation_groups(rows) do
    rows
    |> Enum.reject(&is_nil(&1.subject_id))
    |> Enum.group_by(&{&1.tenant_id, &1.subject_id})
    |> Map.to_list()
  end

  defp access_graph_store(opts) do
    Keyword.get(opts, :access_graph_store) ||
      Application.get_env(:mezzanine_leasing, :access_graph_store)
  end

  defp access_graph_revoke_opts(rows, opts) do
    first = List.first(rows)

    [
      cause: "lease_revoked",
      trace_id: Keyword.get(opts, :trace_id) || first.trace_id,
      source_node_ref: Keyword.get(opts, :source_node_ref),
      commit_hlc: Keyword.get(opts, :commit_hlc),
      metadata: %{
        "lease_revocations" =>
          rows
          |> Enum.sort_by(& &1.sequence_number)
          |> Enum.map(&lease_revocation_metadata/1)
      }
    ]
    |> maybe_put_test_pid(opts)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp maybe_put_test_pid(graph_opts, opts) do
    case Keyword.get(opts, :access_graph_test_pid) do
      nil -> graph_opts
      pid -> Keyword.put(graph_opts, :access_graph_test_pid, pid)
    end
  end

  defp lease_revocation_metadata(%LeaseInvalidation{} = invalidation) do
    %{
      "lease_kind" => invalidation.lease_kind,
      "reason" => invalidation.reason,
      "revocation_ref" => invalidation.revocation_ref
    }
  end

  defp default_revoking_authority_ref([%LeaseInvalidation{} = invalidation | _rows]) do
    %{
      kind: :policy_decision,
      id: invalidation.revocation_ref,
      subject: %{
        kind: :install,
        id: invalidation.installation_id,
        metadata: %{tenant_id: invalidation.tenant_id}
      },
      evidence: [],
      metadata: %{
        reason: invalidation.reason,
        trace_id: invalidation.trace_id
      }
    }
  end

  defp fetch_invalidations_after(target, cursor, opts) do
    {sql, params} = invalidations_after_query(cursor, opts)

    case query_invalidations(target, sql, params) do
      {:ok, result} ->
        {:ok, Enum.map(result_rows(result), &invalidation_from_row/1)}

      {:error, error} ->
        {:error, error}
    end
  end

  defp invalidations_after_query(cursor, opts) do
    filters = [
      {:lease_id, Keyword.get(opts, :lease_id)},
      {:lease_kind, normalize_optional_lease_kind(Keyword.get(opts, :lease_kind))}
    ]

    {clauses, params, _index} =
      Enum.reduce(filters, {[], [cursor], 2}, fn
        {_field, nil}, acc ->
          acc

        {field, value}, {clauses, params, index} ->
          clause = invalidation_filter_clause(field, index)
          {[clause | clauses], params ++ [invalidation_filter_value(field, value)], index + 1}
      end)

    limit = Keyword.get(opts, :limit)

    sql =
      [
        """
        SELECT id, lease_id, lease_kind, tenant_id, installation_id,
               installation_revision, activation_epoch, lease_epoch, subject_id,
               execution_id, trace_id, reason, sequence_number, revocation_ref,
               cache_invalidation_ref, invalidated_at, inserted_at
        FROM lease_invalidations
        WHERE sequence_number > $1
        """,
        if(clauses == [],
          do: "",
          else: "\n  AND " <> Enum.join(Enum.reverse(clauses), "\n  AND ")
        ),
        "\nORDER BY sequence_number ASC",
        if(is_integer(limit), do: "\nLIMIT #{limit}", else: "")
      ]
      |> IO.iodata_to_binary()

    {sql, params}
  end

  defp invalidation_from_row(row) do
    %LeaseInvalidation{
      id: row.id,
      lease_id: row.lease_id,
      lease_kind: row.lease_kind,
      tenant_id: row.tenant_id,
      installation_id: row.installation_id,
      installation_revision: row.installation_revision,
      activation_epoch: row.activation_epoch,
      lease_epoch: row.lease_epoch,
      subject_id: row.subject_id,
      execution_id: row.execution_id,
      trace_id: row.trace_id,
      reason: row.reason,
      sequence_number: row.sequence_number,
      revocation_ref: row.revocation_ref,
      cache_invalidation_ref: row.cache_invalidation_ref,
      invalidated_at: coerce_datetime(row.invalidated_at),
      inserted_at: coerce_datetime(row.inserted_at)
    }
  end

  defp invalidation_filter_clause(:lease_id, index), do: "lease_id = $#{index}::uuid"
  defp invalidation_filter_clause(:subject_id, index), do: "subject_id = $#{index}::uuid"
  defp invalidation_filter_clause(:execution_id, index), do: "execution_id = $#{index}::uuid"
  defp invalidation_filter_clause(field, index), do: "#{Atom.to_string(field)} = $#{index}"

  defp invalidation_filter_value(:lease_id, value), do: dump_uuid!(value)
  defp invalidation_filter_value(:subject_id, value), do: dump_uuid!(value)
  defp invalidation_filter_value(:execution_id, value), do: dump_uuid!(value)
  defp invalidation_filter_value(_field, value), do: value

  defp normalize_optional_lease_kind(nil), do: nil
  defp normalize_optional_lease_kind(:read), do: "read"
  defp normalize_optional_lease_kind(:stream), do: "stream"
  defp normalize_optional_lease_kind(value) when is_binary(value), do: value

  defp lock_invalidation_writes(repo) do
    case repo.query(@invalidation_lock_sql, []) do
      {:ok, _result} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp current_invalidation_cursor(repo) when is_atom(repo) do
    repo.aggregate(LeaseInvalidation, :max, :sequence_number) || 0
  end

  defp result_rows(%Postgrex.Result{columns: columns, rows: rows}) do
    Enum.map(rows, fn row ->
      Enum.zip(columns, row)
      |> Map.new(fn {key, value} -> {Map.get(@invalidation_result_columns, key, key), value} end)
    end)
  end

  defp coerce_datetime(%DateTime{} = value), do: value

  defp coerce_datetime(%NaiveDateTime{} = value) do
    DateTime.from_naive!(value, "Etc/UTC")
  end

  defp coerce_datetime(value), do: value

  defp dump_uuid!(value), do: Ecto.UUID.dump!(value)

  defp query_target(opts) do
    case Keyword.get(opts, :connection) do
      nil -> repo(opts)
      connection -> {:connection, connection}
    end
  end

  defp query_invalidations({:connection, connection}, sql, params) do
    Postgrex.query(connection, sql, params)
  end

  defp query_invalidations(repo, sql, params) do
    repo.query(sql, params)
  end

  defp advance_stream_cursor_via_connection(connection, lease_id, sequence_number, now) do
    sql = """
    UPDATE stream_attach_leases
    SET last_invalidation_cursor = $1,
        updated_at = $2
    WHERE lease_id = $3::uuid
      AND (last_invalidation_cursor IS NULL OR last_invalidation_cursor < $4)
    """

    case Postgrex.query(connection, sql, [
           sequence_number,
           now,
           dump_uuid!(lease_id),
           sequence_number
         ]) do
      {:ok, %Postgrex.Result{num_rows: count}} when count in [0, 1] ->
        :ok

      {:ok, %Postgrex.Result{num_rows: _count}} ->
        {:error, :unexpected_cursor_update_count}

      {:error, error} ->
        {:error, error}
    end
  end

  defp emit_lease_issued(lease) do
    Telemetry.emit(
      [:lease, :issued],
      %{count: 1},
      %{
        lease_id: lease.lease_id,
        trace_id: lease.trace_id,
        tenant_id: lease.tenant_id,
        installation_id: Map.get(lease, :installation_id),
        subject_id: Map.get(lease, :subject_id),
        execution_id: Map.get(lease, :execution_id),
        lease_kind: lease_kind(lease),
        allowed_family: Map.get(lease, :allowed_family),
        issued_invalidation_cursor: Map.get(lease, :issued_invalidation_cursor),
        invalidation_channel: Map.get(lease, :invalidation_channel),
        expires_at: Map.get(lease, :expires_at)
      }
    )
  end

  defp emit_lease_invalidated(%LeaseInvalidation{} = invalidation) do
    Telemetry.emit(
      [:lease, :invalidated],
      %{count: 1},
      %{
        lease_id: invalidation.lease_id,
        trace_id: invalidation.trace_id,
        tenant_id: invalidation.tenant_id,
        installation_id: invalidation.installation_id,
        subject_id: invalidation.subject_id,
        execution_id: invalidation.execution_id,
        lease_kind: invalidation.lease_kind,
        reason: invalidation.reason,
        sequence_number: invalidation.sequence_number,
        invalidated_at: invalidation.invalidated_at
      }
    )
  end

  defp lease_kind(%ReadLease{}), do: "read"
  defp lease_kind(%StreamAttachLease{}), do: "stream"

  defp repo(opts) do
    Keyword.get(opts, :repo, Mezzanine.Leasing.Repo)
  end

  defp normalize_attrs(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize_attrs(attrs) when is_map(attrs), do: attrs

  defp fetch_required!(attrs, key) do
    case Map.fetch(attrs, key) do
      {:ok, value} when not is_nil(value) -> value
      _missing -> raise ArgumentError, "missing required lease attribute #{inspect(key)}"
    end
  end

  defp verify_token(stored_digest, token) when is_binary(stored_digest) and is_binary(token) do
    if secure_compare(stored_digest, digest_token(token)) do
      :ok
    else
      {:error, :invalid_lease_token}
    end
  end

  defp generate_token do
    32
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp digest_token(token) do
    :sha256
    |> :crypto.hash(token)
    |> Base.encode16(case: :lower)
  end

  defp secure_compare(left, right) when is_binary(left) and is_binary(right) do
    byte_size(left) == byte_size(right) and :crypto.hash_equals(left, right)
  end

  defp normalize_operations(operations) do
    operations
    |> Enum.map(&normalize_operation/1)
    |> Enum.uniq()
  end

  defp normalize_operation(operation) when is_atom(operation), do: Atom.to_string(operation)
  defp normalize_operation(operation) when is_binary(operation), do: operation

  defp invalidation_channel(prefix, family, trace_id) do
    "#{prefix}:#{family}:#{trace_id}"
  end

  defp now(opts) do
    Keyword.get(opts, :now, DateTime.utc_now())
  end

  defp default_read_ttl_ms do
    Application.get_env(:mezzanine_leasing, :default_read_ttl_ms, 300_000)
  end

  defp default_stream_ttl_ms do
    Application.get_env(:mezzanine_leasing, :default_stream_ttl_ms, 120_000)
  end
end
