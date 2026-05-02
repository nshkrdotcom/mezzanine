defmodule Mezzanine.WorkflowRuntime.DeterministicCodexReceipt do
  @moduledoc """
  Deterministic Codex-shaped receipt activity for Phase 9.

  This module is intentionally fixture-only. It reads a local deterministic
  fixture and a local Temporal state file, then emits compact workflow facts
  that match Codex completion, failure, stall, user-input, token replay, and
  rate-limit cases without calling a provider, connector, or network client.
  """

  @activity_version "Mezzanine.DeterministicCodexReceiptActivity.v1"
  @fixture_schema "mezzanine.codex.receipts.phase9.v1"
  @forbidden_live_fields [
    :provider_adapter,
    :provider_sdk,
    :provider_client,
    :codex_provider_call,
    :linear_adapter,
    :linear_client,
    :github_adapter,
    :github_client,
    :network_client,
    :http_client,
    :connector_operation,
    :live_provider_call,
    :live_linear_call,
    :live_github_call
  ]
  @required_activity_fields [
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :workflow_id,
    :workflow_run_id,
    :activity_call_ref,
    :fixture_path,
    :temporal_address,
    :temporal_namespace,
    :temporal_state_db_path,
    :trace_id,
    :idempotency_key,
    :release_manifest_ref
  ]

  @doc "Static activity contract for deterministic Codex receipt proof."
  @spec contract() :: map()
  def contract do
    %{
      activity_version: @activity_version,
      workflow_runtime_owner: :mezzanine,
      lower_runtime_shape: :codex,
      allowed_input_sources: [:local_fixture, :temporal_state_file],
      live_external_io_allowed?: false,
      forbidden_live_fields: @forbidden_live_fields,
      mapped_fact_states: [
        :completed,
        :failed,
        :stalled,
        :user_input_required,
        :rate_limited
      ],
      token_replay_policy: :dedupe_by_idempotency_key
    }
  end

  @doc "Read local deterministic inputs and emit workflow-safe Codex receipt facts."
  @spec run_activity(map() | keyword()) :: {:ok, map()} | {:error, term()}
  def run_activity(attrs) do
    attrs = normalize(attrs)

    with :ok <- reject_live_external_io(attrs),
         :ok <- require_fields(attrs, @required_activity_fields),
         {:ok, fixture_path} <- local_file_path(value(attrs, :fixture_path), :fixture),
         {:ok, temporal_state_path} <-
           local_file_path(value(attrs, :temporal_state_db_path), :temporal_state),
         {:ok, fixture} <- read_fixture(fixture_path),
         :ok <- validate_fixture(fixture, attrs),
         {:ok, temporal_state} <- temporal_state(attrs, temporal_state_path),
         {:ok, receipt_facts} <- receipt_facts(fixture),
         {:ok, token_dedupe} <- token_dedupe(Map.get(fixture, "token_events", [])) do
      {:ok,
       %{
         contract_name: @activity_version,
         owner_repo: :mezzanine,
         activity: :deterministic_codex_receipt,
         activity_call_ref: value(attrs, :activity_call_ref),
         tenant_ref: value(attrs, :tenant_ref),
         installation_ref: value(attrs, :installation_ref),
         workspace_ref: value(attrs, :workspace_ref),
         workflow_id: value(attrs, :workflow_id),
         workflow_run_id: value(attrs, :workflow_run_id),
         trace_id: value(attrs, :trace_id),
         idempotency_key: value(attrs, :idempotency_key),
         release_manifest_ref: value(attrs, :release_manifest_ref),
         fixture_ref: Map.fetch!(fixture, "fixture_ref"),
         fixture_schema: Map.fetch!(fixture, "fixture_schema"),
         io_policy: io_policy(fixture_path),
         temporal_substrate: temporal_state,
         receipt_facts: receipt_facts,
         fact_counts: fact_counts(receipt_facts),
         token_dedupe: token_dedupe,
         projection_state: "projected",
         workflow_effect_state: "deterministic_receipts_mapped"
       }}
    end
  end

  defp reject_live_external_io(attrs) do
    case Enum.find(@forbidden_live_fields, &present?(value(attrs, &1))) do
      nil -> :ok
      field -> {:error, {:live_external_io_forbidden, field}}
    end
  end

  defp require_fields(attrs, fields) do
    case Enum.reject(fields, &present?(value(attrs, &1))) do
      [] -> :ok
      missing -> {:error, {:missing_required_fields, missing}}
    end
  end

  defp local_file_path(path, kind) when is_binary(path) do
    if non_local_path?(path) do
      {:error, non_local_error(kind, path)}
    else
      expanded = Path.expand(path, File.cwd!())

      if File.regular?(expanded) do
        {:ok, expanded}
      else
        {:error, missing_local_error(kind)}
      end
    end
  end

  defp local_file_path(path, kind), do: {:error, non_local_error(kind, path)}

  defp non_local_path?(path) do
    Enum.any?(
      ["http://", "https://", "git://", "ssh://", "app://"],
      &String.starts_with?(path, &1)
    )
  end

  defp non_local_error(:fixture, path), do: {:non_local_fixture_path, path}
  defp non_local_error(:temporal_state, path), do: {:non_local_temporal_state_path, path}
  defp missing_local_error(:fixture), do: :fixture_file_missing
  defp missing_local_error(:temporal_state), do: :temporal_state_db_missing

  defp read_fixture(path) do
    with {:ok, bytes} <- File.read(path),
         {:ok, fixture} <- decode_json(bytes),
         true <- is_map(fixture) do
      {:ok, fixture}
    else
      {:error, reason} -> {:error, {:invalid_fixture, reason}}
      _other -> {:error, :invalid_fixture}
    end
  end

  defp decode_json(bytes) do
    {:ok, :json.decode(bytes)}
  rescue
    error in ErlangError -> {:error, error.original}
  end

  defp validate_fixture(fixture, attrs) do
    results = [
      require_fixture_field(fixture, "fixture_schema", @fixture_schema),
      require_fixture_field(fixture, "fixture_ref"),
      require_fixture_field(fixture, "receipts"),
      require_temporal_fixture(fixture, attrs)
    ]

    case Enum.find(results, &(&1 != :ok)) do
      nil -> :ok
      error -> error
    end
  end

  defp require_fixture_field(fixture, field) do
    if present?(Map.get(fixture, field)), do: :ok, else: {:error, {:missing_fixture_field, field}}
  end

  defp require_fixture_field(fixture, field, expected) do
    case Map.get(fixture, field) do
      ^expected -> :ok
      actual -> {:error, {:fixture_field_mismatch, field, expected, actual}}
    end
  end

  defp require_temporal_fixture(fixture, attrs) do
    temporal = Map.get(fixture, "temporal", %{})

    cond do
      Map.get(temporal, "address") != value(attrs, :temporal_address) ->
        {:error,
         {:fixture_field_mismatch, "temporal.address", value(attrs, :temporal_address),
          Map.get(temporal, "address")}}

      Map.get(temporal, "namespace") != value(attrs, :temporal_namespace) ->
        {:error,
         {:fixture_field_mismatch, "temporal.namespace", value(attrs, :temporal_namespace),
          Map.get(temporal, "namespace")}}

      true ->
        :ok
    end
  end

  defp temporal_state(attrs, state_path) do
    with {:ok, stat} <- File.stat(state_path),
         {:ok, state_ref} <- file_sha256(state_path) do
      {:ok,
       %{
         source: :temporal_state_file,
         address: value(attrs, :temporal_address),
         namespace: value(attrs, :temporal_namespace),
         service: "mezzanine-temporal-dev.service",
         state_db_present?: true,
         state_db_ref: state_ref,
         state_db_size_bytes: stat.size
       }}
    else
      {:error, _reason} -> {:error, :temporal_state_db_missing}
    end
  end

  defp io_policy(fixture_path) do
    %{
      live_external_io_allowed?: false,
      allowed_input_sources: [:local_fixture, :temporal_state_file],
      fixture_ref: "sha256:#{sha256!(File.read!(fixture_path))}",
      forbidden_live_fields_checked: @forbidden_live_fields,
      provider_call: :forbidden,
      linear_call: :forbidden,
      github_call: :forbidden,
      connector_operation: :forbidden,
      network_client: :forbidden
    }
  end

  defp receipt_facts(%{"receipts" => receipts}) when is_list(receipts) do
    receipts
    |> Enum.with_index(1)
    |> reduce_receipt_facts()
  end

  defp receipt_facts(_fixture), do: {:error, :invalid_receipts_fixture}

  defp reduce_receipt_facts(receipts) do
    Enum.reduce_while(receipts, {:ok, []}, fn {receipt, index}, {:ok, facts} ->
      case receipt_fact(receipt, index) do
        {:ok, fact} -> {:cont, {:ok, facts ++ [fact]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp receipt_fact(receipt, index) when is_map(receipt) do
    with {:ok, state} <- required_string(receipt, "state"),
         {:ok, receipt_ref} <- required_string(receipt, "receipt_ref"),
         {:ok, event_ref} <- required_string(receipt, "event_ref"),
         {:ok, idempotency_key} <- required_string(receipt, "idempotency_key") do
      base = %{
        fact_ref: "codex-fact://phase9/#{index}",
        receipt_ref: receipt_ref,
        lower_receipt_ref: Map.get(receipt, "lower_receipt_ref"),
        event_ref: event_ref,
        idempotency_key: idempotency_key,
        receipt_state: state,
        terminal?: Map.get(receipt, "terminal", false),
        evidence_refs: Map.get(receipt, "evidence_refs", []),
        output_artifact_refs: Map.get(receipt, "output_artifact_refs", []),
        source_contract_ref: @fixture_schema
      }

      state_fact(base, receipt)
    end
  end

  defp receipt_fact(_receipt, _index), do: {:error, :invalid_receipt_fixture}

  defp state_fact(%{receipt_state: "completed"} = base, _receipt) do
    {:ok,
     Map.merge(base, %{
       fact_kind: :codex_completion,
       workflow_effect_state: "completed",
       projection_state: "projected",
       safe_action: "finalize"
     })}
  end

  defp state_fact(%{receipt_state: "failed"} = base, receipt) do
    {:ok,
     Map.merge(base, %{
       fact_kind: :codex_failure,
       failure_class: Map.get(receipt, "failure_class", "codex_execution_failed"),
       workflow_effect_state: "failed",
       projection_state: "projected",
       safe_action: "operator_review"
     })}
  end

  defp state_fact(%{receipt_state: "stalled"} = base, receipt) do
    {:ok,
     Map.merge(base, %{
       fact_kind: :codex_stall,
       heartbeat_state: "stalled",
       stall_timeout_ms: Map.get(receipt, "stall_timeout_ms"),
       last_heartbeat_ref: Map.get(receipt, "last_heartbeat_ref"),
       workflow_effect_state: "retry_scheduled",
       projection_state: "projected",
       safe_action: "retry_or_cancel"
     })}
  end

  defp state_fact(%{receipt_state: "user_input_required"} = base, receipt) do
    {:ok,
     Map.merge(base, %{
       fact_kind: :codex_user_input_required,
       input_request_ref: Map.get(receipt, "input_request_ref"),
       workflow_effect_state: "blocked",
       projection_state: "projected",
       safe_action: "operator_review"
     })}
  end

  defp state_fact(%{receipt_state: "rate_limited"} = base, receipt) do
    {:ok,
     Map.merge(base, %{
       fact_kind: :codex_rate_limited,
       retry_after_ms: Map.get(receipt, "retry_after_ms"),
       workflow_effect_state: "backoff_scheduled",
       projection_state: "projected",
       safe_action: "backoff"
     })}
  end

  defp state_fact(%{receipt_state: state}, _receipt),
    do: {:error, {:unknown_receipt_state, state}}

  defp fact_counts(facts) do
    Enum.reduce(
      facts,
      %{completed: 0, failed: 0, stalled: 0, user_input_required: 0, rate_limited: 0},
      fn fact, acc -> Map.update!(acc, count_key(fact.receipt_state), &(&1 + 1)) end
    )
  end

  defp count_key("completed"), do: :completed
  defp count_key("failed"), do: :failed
  defp count_key("stalled"), do: :stalled
  defp count_key("user_input_required"), do: :user_input_required
  defp count_key("rate_limited"), do: :rate_limited

  defp token_dedupe(events) when is_list(events) do
    result =
      Enum.reduce(events, empty_token_dedupe(), fn event, acc ->
        event = normalize_event(event)
        key = Map.get(event, "idempotency_key")

        cond do
          not present?(key) ->
            Map.update!(acc, :invalid_count, &(&1 + 1))

          MapSet.member?(acc.seen_keys, key) ->
            acc
            |> Map.update!(:duplicate_count, &(&1 + 1))
            |> Map.update!(:duplicate_event_refs, &(&1 ++ [Map.get(event, "event_ref")]))

          true ->
            token_hash = token_hash_ref(event)

            acc
            |> Map.update!(:accepted_count, &(&1 + 1))
            |> Map.update!(:accepted_event_refs, &(&1 ++ [Map.get(event, "event_ref")]))
            |> Map.update!(:token_hash_refs, &(&1 ++ [token_hash]))
            |> Map.update!(:seen_keys, &MapSet.put(&1, key))
        end
      end)

    {:ok, Map.delete(result, :seen_keys)}
  end

  defp token_dedupe(_events), do: {:error, :invalid_token_events_fixture}

  defp empty_token_dedupe do
    %{
      accepted_count: 0,
      duplicate_count: 0,
      invalid_count: 0,
      accepted_event_refs: [],
      duplicate_event_refs: [],
      token_hash_refs: [],
      seen_keys: MapSet.new()
    }
  end

  defp normalize_event(event) when is_map(event), do: event
  defp normalize_event(_event), do: %{}

  defp token_hash_ref(event) do
    "sha256:#{sha256!(Map.get(event, "token_text", ""))}"
  end

  defp required_string(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _missing -> {:error, {:missing_fixture_field, key}}
    end
  end

  defp file_sha256(path) do
    case File.read(path) do
      {:ok, bytes} -> {:ok, "sha256:#{sha256!(bytes)}"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp sha256!(bytes) do
    :crypto.hash(:sha256, bytes)
    |> Base.encode16(case: :lower)
  end

  defp normalize(attrs) when is_list(attrs), do: Map.new(attrs)
  defp normalize(%_{} = attrs), do: Map.from_struct(attrs)
  defp normalize(attrs) when is_map(attrs), do: attrs

  defp value(attrs, key, default \\ nil) when is_atom(key) do
    Map.get(attrs, key, Map.get(attrs, Atom.to_string(key), default))
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(value) when is_list(value), do: value != []
  defp present?(value) when is_map(value), do: map_size(value) > 0
  defp present?(value), do: not is_nil(value)
end

defmodule Mezzanine.Activities.DeterministicCodexReceipt do
  @moduledoc "Temporal activity wrapper for deterministic Phase 9 Codex receipt fixtures."

  use Temporalex.Activity,
    task_queue: "mezzanine.hazmat",
    start_to_close_timeout: 10_000,
    retry_policy: [max_attempts: 1]

  alias Mezzanine.WorkflowRuntime.DeterministicCodexReceipt

  @impl Temporalex.Activity
  def perform(input), do: DeterministicCodexReceipt.run_activity(input)
end
