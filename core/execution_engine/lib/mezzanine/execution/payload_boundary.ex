defmodule Mezzanine.Execution.PayloadBoundary do
  @moduledoc """
  Local durable payload boundary for workflow-visible execution maps.

  Phase 5 does not add a new store here. Large or provider-shaped payloads must
  already be represented by source-owned artifact references before they reach
  the execution ledger.
  """

  @small_inline_max_bytes 64 * 1024
  @reject_or_stream_min_bytes 5 * 1024 * 1024

  @artifact_ref_fields [
    "artifact_id",
    "content_hash",
    "content_hash_alg",
    "byte_size",
    "schema_name",
    "schema_hash",
    "schema_hash_alg",
    "media_type",
    "producer_repo",
    "tenant_scope",
    "sensitivity_class",
    "store_security_posture_ref",
    "encryption_posture_ref",
    "retrieval_owner",
    "existing_fetch_or_restore_path",
    "safe_actions",
    "queue_key",
    "oversize_action",
    "release_manifest_ref"
  ]
  @artifact_ref_field_lookup %{
    "artifact_id" => :artifact_id,
    "content_hash" => :content_hash,
    "content_hash_alg" => :content_hash_alg,
    "byte_size" => :byte_size,
    "schema_name" => :schema_name,
    "schema_hash" => :schema_hash,
    "schema_hash_alg" => :schema_hash_alg,
    "media_type" => :media_type,
    "producer_repo" => :producer_repo,
    "tenant_scope" => :tenant_scope,
    "sensitivity_class" => :sensitivity_class,
    "store_security_posture_ref" => :store_security_posture_ref,
    "encryption_posture_ref" => :encryption_posture_ref,
    "retrieval_owner" => :retrieval_owner,
    "existing_fetch_or_restore_path" => :existing_fetch_or_restore_path,
    "safe_actions" => :safe_actions,
    "queue_key" => :queue_key,
    "oversize_action" => :oversize_action,
    "release_manifest_ref" => :release_manifest_ref
  }

  @phase5_lifecycle_fields ["storage_tier", "retention_class", "fetch_policy"]
  @raw_payload_fields [
    "artifact_bytes",
    "lower_stderr",
    "lower_stdout",
    "prompt",
    "prompts",
    "provider_error_body",
    "provider_native_body",
    "provider_response",
    "raw_body",
    "raw_payload",
    "raw_provider_body",
    "secret_stack_dump",
    "secrets",
    "stack_dump_with_secrets",
    "stderr",
    "stdout",
    "tool_output",
    "tool_outputs"
  ]
  @sensitive_classes ["tenant_sensitive", "secret_sensitive", "regulated_sensitive"]
  @fetch_or_restore_paths [
    "existing_claim_check_fetch",
    "archival_restore_owner",
    "incident_export_owner",
    "unavailable_fail_closed"
  ]

  @type classification :: :small_inline | :ref_required | :reject_or_stream

  @spec small_inline_max_bytes() :: pos_integer()
  def small_inline_max_bytes, do: @small_inline_max_bytes

  @spec reject_or_stream_min_bytes() :: pos_integer()
  def reject_or_stream_min_bytes, do: @reject_or_stream_min_bytes

  @spec classify_execution_column(atom(), map()) ::
          {:ok, classification()} | {:error, term()}
  def classify_execution_column(column, payload) when is_atom(column) and is_map(payload) do
    with :ok <- reject_phase5_lifecycle_fields(column, payload),
         :ok <- reject_raw_payload_fields(column, payload),
         artifact_refs = collect_artifact_refs(payload),
         :ok <- validate_artifact_refs(column, artifact_refs) do
      case artifact_refs do
        [] -> classify_inline_payload(column, payload)
        [_ | _] -> {:ok, :ref_required}
      end
    end
  end

  def classify_execution_column(column, _payload) when is_atom(column) do
    boundary_error(column, :invalid_map_column, %{
      classification: :reject_or_stream,
      safe_action: :reject_before_durable_write
    })
  end

  @spec validate_execution_column(atom(), map()) :: :ok | {:error, term()}
  def validate_execution_column(column, payload) do
    case classify_execution_column(column, payload) do
      {:ok, _classification} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp classify_inline_payload(column, payload) do
    byte_size = :erlang.external_size(payload)

    cond do
      byte_size < @small_inline_max_bytes ->
        {:ok, :small_inline}

      byte_size <= @reject_or_stream_min_bytes ->
        boundary_error(column, :artifact_ref_required, %{
          byte_size: byte_size,
          max_inline_bytes: @small_inline_max_bytes,
          classification: :ref_required,
          safe_action: :reject_before_durable_write
        })

      true ->
        boundary_error(column, :reject_or_stream_required, %{
          byte_size: byte_size,
          max_ref_required_bytes: @reject_or_stream_min_bytes,
          classification: :reject_or_stream,
          safe_action: :reject_before_durable_write
        })
    end
  end

  defp reject_phase5_lifecycle_fields(column, payload) do
    case find_key(payload, @phase5_lifecycle_fields) do
      nil ->
        :ok

      field ->
        boundary_error(column, :phase5_lifecycle_field_forbidden, %{
          field: field,
          classification: :reject_or_stream,
          safe_action: :reject_before_durable_write
        })
    end
  end

  defp reject_raw_payload_fields(column, payload) do
    case find_key(payload, @raw_payload_fields) do
      nil ->
        :ok

      field ->
        boundary_error(column, :raw_payload_forbidden, %{
          field: field,
          classification: :reject_or_stream,
          safe_action: :reject_before_durable_write
        })
    end
  end

  defp validate_artifact_refs(_column, []), do: :ok

  defp validate_artifact_refs(column, artifact_refs) do
    Enum.reduce_while(artifact_refs, :ok, fn artifact_ref, :ok ->
      case validate_artifact_ref(column, artifact_ref) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp validate_artifact_ref(column, artifact_ref) do
    with :ok <- require_artifact_ref_fields(column, artifact_ref),
         :ok <- validate_primary_hash(column, artifact_ref, "content_hash", "content_hash_alg"),
         :ok <- validate_primary_hash(column, artifact_ref, "schema_hash", "schema_hash_alg"),
         :ok <- validate_byte_size(column, artifact_ref),
         :ok <- validate_fetch_or_restore_path(column, artifact_ref),
         :ok <- validate_sensitivity_posture(column, artifact_ref) do
      validate_safe_actions(column, artifact_ref)
    end
  end

  defp require_artifact_ref_fields(column, artifact_ref) do
    missing =
      Enum.reject(@artifact_ref_fields, fn field ->
        value = field_value(artifact_ref, field)
        not is_nil(value) and value != "" and value != []
      end)

    cond do
      missing == [] ->
        :ok

      sensitive_posture_missing?(artifact_ref, missing) ->
        boundary_error(column, :missing_sensitive_posture, %{
          classification: :ref_required,
          safe_action: :unavailable_fail_closed
        })

      true ->
        boundary_error(column, :missing_artifact_ref_fields, %{
          fields: missing,
          classification: :ref_required,
          safe_action: :reject_before_durable_write
        })
    end
  end

  defp sensitive_posture_missing?(artifact_ref, missing) do
    field_value(artifact_ref, "sensitivity_class") in @sensitive_classes and
      Enum.any?(missing, &(&1 in ["store_security_posture_ref", "encryption_posture_ref"]))
  end

  defp validate_primary_hash(column, artifact_ref, hash_field, alg_field) do
    hash = field_value(artifact_ref, hash_field)
    alg = field_value(artifact_ref, alg_field)

    cond do
      alg != "sha256" ->
        boundary_error(column, :invalid_primary_hash, %{
          field: alg_field,
          algorithm: alg,
          classification: :ref_required,
          safe_action: :reject_before_durable_write
        })

      not sha256_ref?(hash) ->
        boundary_error(column, :invalid_primary_hash, %{
          field: hash_field,
          classification: :ref_required,
          safe_action: :reject_before_durable_write
        })

      true ->
        :ok
    end
  end

  defp validate_byte_size(column, artifact_ref) do
    case field_value(artifact_ref, "byte_size") do
      value when is_integer(value) and value >= 0 ->
        :ok

      value ->
        boundary_error(column, :invalid_byte_size, %{
          byte_size: value,
          classification: :ref_required,
          safe_action: :reject_before_durable_write
        })
    end
  end

  defp validate_fetch_or_restore_path(column, artifact_ref) do
    case field_value(artifact_ref, "existing_fetch_or_restore_path") do
      value when value in @fetch_or_restore_paths ->
        :ok

      value ->
        boundary_error(column, :invalid_fetch_or_restore_path, %{
          path: value,
          classification: :ref_required,
          safe_action: :reject_before_durable_write
        })
    end
  end

  defp validate_sensitivity_posture(column, artifact_ref) do
    sensitivity_class = field_value(artifact_ref, "sensitivity_class")
    store_security = field_value(artifact_ref, "store_security_posture_ref")
    encryption = field_value(artifact_ref, "encryption_posture_ref")

    cond do
      not is_binary(sensitivity_class) or sensitivity_class == "" ->
        boundary_error(column, :missing_sensitivity_class, %{
          classification: :ref_required,
          safe_action: :reject_before_durable_write
        })

      sensitivity_class in @sensitive_classes and
          (blank?(store_security) or blank?(encryption)) ->
        boundary_error(column, :missing_sensitive_posture, %{
          classification: :ref_required,
          safe_action: :unavailable_fail_closed
        })

      true ->
        :ok
    end
  end

  defp validate_safe_actions(column, artifact_ref) do
    case field_value(artifact_ref, "safe_actions") do
      actions when is_list(actions) and actions != [] ->
        :ok

      actions ->
        boundary_error(column, :invalid_safe_actions, %{
          safe_actions: actions,
          classification: :ref_required,
          safe_action: :reject_before_durable_write
        })
    end
  end

  defp collect_artifact_refs(value) when is_map(value) do
    if artifact_ref?(value) do
      [value]
    else
      value
      |> Map.values()
      |> Enum.flat_map(&collect_artifact_refs/1)
    end
  end

  defp collect_artifact_refs(value) when is_list(value),
    do: Enum.flat_map(value, &collect_artifact_refs/1)

  defp collect_artifact_refs(_value), do: []

  defp artifact_ref?(value) when is_map(value) do
    present?(value, "artifact_id") and present?(value, "content_hash") and
      present?(value, "schema_hash")
  end

  defp find_key(value, forbidden_keys) when is_map(value) do
    Enum.find_value(value, fn {key, nested_value} ->
      normalized_key = normalize_key(key)

      cond do
        normalized_key in forbidden_keys -> normalized_key
        is_map(nested_value) or is_list(nested_value) -> find_key(nested_value, forbidden_keys)
        true -> nil
      end
    end)
  end

  defp find_key(value, forbidden_keys) when is_list(value) do
    Enum.find_value(value, &find_key(&1, forbidden_keys))
  end

  defp find_key(_value, _forbidden_keys), do: nil

  defp present?(map, field), do: not blank?(field_value(map, field))

  defp blank?(value), do: is_nil(value) or value == "" or value == []

  defp sha256_ref?(<<"sha256:", digest::binary-size(64)>>), do: lower_hex?(digest)
  defp sha256_ref?(_hash), do: false

  defp lower_hex?(value) do
    value
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end

  defp field_value(map, field) when is_binary(field),
    do: Map.get(map, field) || Map.get(map, Map.get(@artifact_ref_field_lookup, field))

  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key) when is_binary(key), do: key
  defp normalize_key(key), do: inspect(key)

  defp boundary_error(column, reason, details) do
    {:error, {:execution_payload_boundary, column, Map.put(details, :reason, reason)}}
  end
end
