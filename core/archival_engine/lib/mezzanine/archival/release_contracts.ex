defmodule Mezzanine.Archival.ReleaseContractSupport do
  @moduledoc false

  @base_required_binary_fields [
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref
  ]

  @optional_actor_fields [:principal_ref, :system_actor_ref]
  @sha256_regex ~r/\Asha256:[0-9a-f]{64}\z/

  @spec base_required_binary_fields() :: [atom()]
  def base_required_binary_fields, do: @base_required_binary_fields

  @spec optional_actor_fields() :: [atom()]
  def optional_actor_fields, do: @optional_actor_fields

  @spec normalize_attrs(map() | keyword() | struct()) :: {:ok, map()} | {:error, :invalid_attrs}
  def normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}

  def normalize_attrs(attrs) when is_map(attrs) do
    if Map.has_key?(attrs, :__struct__), do: {:ok, Map.from_struct(attrs)}, else: {:ok, attrs}
  end

  def normalize_attrs(_attrs), do: {:error, :invalid_attrs}

  @spec missing_required_fields(map(), [atom()], [atom()]) :: [atom()]
  def missing_required_fields(attrs, required_binary, required_datetimes) do
    binary_missing =
      required_binary
      |> Enum.reject(fn field -> present_binary?(Map.get(attrs, field)) end)

    datetime_missing =
      required_datetimes
      |> Enum.reject(fn field -> match?(%DateTime{}, Map.get(attrs, field)) end)

    actor_missing =
      if present_binary?(Map.get(attrs, :principal_ref)) or
           present_binary?(Map.get(attrs, :system_actor_ref)) do
        []
      else
        [:principal_ref_or_system_actor_ref]
      end

    binary_missing ++ actor_missing ++ datetime_missing
  end

  @spec optional_binary_fields?(map(), [atom()]) :: boolean()
  def optional_binary_fields?(attrs, fields) do
    Enum.all?(fields, fn field ->
      value = Map.get(attrs, field)
      is_nil(value) or present_binary?(value)
    end)
  end

  @spec enum_atom(term(), [atom()]) :: {:ok, atom()} | :error
  def enum_atom(value, allowed) when is_atom(value) do
    if value in allowed, do: {:ok, value}, else: :error
  end

  def enum_atom(value, allowed) when is_binary(value) do
    allowed
    |> Enum.find(&(Atom.to_string(&1) == value))
    |> case do
      nil -> :error
      atom -> {:ok, atom}
    end
  end

  def enum_atom(_value, _allowed), do: :error

  @spec present_binary?(term()) :: boolean()
  def present_binary?(value), do: is_binary(value) and byte_size(String.trim(value)) > 0

  @spec non_neg_integer?(term()) :: boolean()
  def non_neg_integer?(value), do: is_integer(value) and value >= 0

  @spec sha256?(term()) :: boolean()
  def sha256?(value), do: is_binary(value) and Regex.match?(@sha256_regex, value)

  @spec actor_fields(map()) :: %{
          principal_ref: String.t() | nil,
          system_actor_ref: String.t() | nil
        }
  def actor_fields(attrs) do
    %{
      principal_ref: Map.get(attrs, :principal_ref),
      system_actor_ref: Map.get(attrs, :system_actor_ref)
    }
  end
end

defmodule Mezzanine.Archival.ColdRestoreTraceQuery do
  @moduledoc """
  Contract for reconstructing archived truth by trace id.

  Contract: `Mezzanine.ColdRestoreTraceQuery.v1`.
  """

  alias Mezzanine.Archival.ReleaseContractSupport

  @contract_name "Mezzanine.ColdRestoreTraceQuery.v1"
  @required_binary_fields ReleaseContractSupport.base_required_binary_fields() ++
                            [
                              :restore_request_ref,
                              :archive_partition_ref,
                              :hot_index_ref,
                              :cold_object_ref,
                              :restore_consistency_hash
                            ]
  @optional_binary_fields ReleaseContractSupport.optional_actor_fields() ++
                            [:retention_policy_ref, :cold_storage_uri_ref]

  defstruct [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :restore_request_ref,
    :archive_partition_ref,
    :hot_index_ref,
    :cold_object_ref,
    :restore_consistency_hash,
    :retention_policy_ref,
    :cold_storage_uri_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_cold_restore_trace_query}
  def new(attrs) do
    with {:ok, attrs} <- ReleaseContractSupport.normalize_attrs(attrs),
         [] <- ReleaseContractSupport.missing_required_fields(attrs, @required_binary_fields, []),
         true <- ReleaseContractSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- ReleaseContractSupport.sha256?(Map.get(attrs, :restore_consistency_hash)) do
      {:ok, build(attrs)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_cold_restore_trace_query}
    end
  end

  defp build(attrs) do
    actors = ReleaseContractSupport.actor_fields(attrs)
    struct!(__MODULE__, Map.merge(attrs, Map.put(actors, :contract_name, @contract_name)))
  end
end

defmodule Mezzanine.Archival.ColdRestoreArtifactQuery do
  @moduledoc """
  Contract for reconstructing archived truth by artifact id and lineage hash.

  Contract: `Mezzanine.ColdRestoreArtifactQuery.v1`.
  """

  alias Mezzanine.Archival.ReleaseContractSupport

  @contract_name "Mezzanine.ColdRestoreArtifactQuery.v1"
  @required_binary_fields ReleaseContractSupport.base_required_binary_fields() ++
                            [
                              :artifact_id,
                              :artifact_kind,
                              :artifact_hash,
                              :lineage_ref,
                              :archive_object_ref,
                              :restore_validation_ref
                            ]
  @optional_binary_fields ReleaseContractSupport.optional_actor_fields() ++
                            [:retention_policy_ref, :cold_storage_uri_ref]

  defstruct [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :artifact_id,
    :artifact_kind,
    :artifact_hash,
    :lineage_ref,
    :archive_object_ref,
    :restore_validation_ref,
    :retention_policy_ref,
    :cold_storage_uri_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_cold_restore_artifact_query}
  def new(attrs) do
    with {:ok, attrs} <- ReleaseContractSupport.normalize_attrs(attrs),
         [] <- ReleaseContractSupport.missing_required_fields(attrs, @required_binary_fields, []),
         true <- ReleaseContractSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- ReleaseContractSupport.sha256?(Map.get(attrs, :artifact_hash)) do
      {:ok, build(attrs)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_cold_restore_artifact_query}
    end
  end

  defp build(attrs) do
    actors = ReleaseContractSupport.actor_fields(attrs)
    struct!(__MODULE__, Map.merge(attrs, Map.put(actors, :contract_name, @contract_name)))
  end
end

defmodule Mezzanine.Archival.RestoreAuditJoin do
  @moduledoc """
  Contract for joining archival restore evidence to audit inclusion proof.

  Contract: `Mezzanine.RestoreAuditJoin.v1`.
  """

  alias Mezzanine.Archival.ReleaseContractSupport
  alias Mezzanine.Audit.AuditInclusionProof

  @contract_name "Mezzanine.RestoreAuditJoin.v1"
  @required_binary_fields [
    :tenant_ref,
    :installation_ref,
    :trace_id,
    :audit_fact_id,
    :checkpoint_ref,
    :release_manifest_ref,
    :restore_consistency_hash,
    :restore_request_ref
  ]
  @optional_binary_fields [:audit_inclusion_proof_ref, :quarantine_ref]

  defstruct [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :audit_fact_id,
    :checkpoint_ref,
    :release_manifest_ref,
    :restore_consistency_hash,
    :restore_request_ref,
    :audit_inclusion_proof_ref,
    :quarantine_ref,
    :quarantine_reason,
    join_status: :authoritative,
    missing_fields: []
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_restore_audit_join}
  def new(attrs) do
    with {:ok, attrs} <- ReleaseContractSupport.normalize_attrs(attrs),
         [] <- ReleaseContractSupport.missing_required_fields(attrs, @required_binary_fields, []),
         true <- ReleaseContractSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- ReleaseContractSupport.sha256?(Map.get(attrs, :restore_consistency_hash)) do
      {:ok, build(attrs, :authoritative)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_restore_audit_join}
    end
  end

  @spec from_proof(map() | keyword(), struct()) ::
          {:ok, t()}
          | {:error, {:audit_restore_join_mismatch, [atom()]}}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_restore_audit_join}
          | {:error, :invalid_attrs}
  def from_proof(attrs, %AuditInclusionProof{} = proof) do
    with {:ok, attrs} <- ReleaseContractSupport.normalize_attrs(attrs),
         :ok <- validate_proof_match(attrs, proof) do
      attrs
      |> put_proof_fields(proof)
      |> new()
    end
  end

  @spec classify(map() | keyword()) :: {:ok, t()} | {:error, :invalid_attrs}
  def classify(attrs) do
    case new(attrs) do
      {:ok, join} ->
        {:ok, join}

      {:error, {:missing_required_fields, fields}} ->
        build_quarantine(attrs, {:missing_required_fields, fields}, fields)

      {:error, reason} ->
        build_quarantine(attrs, reason, [])
    end
  end

  defp build(attrs, status) do
    struct!(
      __MODULE__,
      attrs
      |> Map.put(:contract_name, @contract_name)
      |> Map.put(:join_status, status)
      |> Map.put_new(:missing_fields, [])
    )
  end

  defp build_quarantine(attrs, reason, missing_fields) do
    with {:ok, attrs} <- ReleaseContractSupport.normalize_attrs(attrs) do
      {:ok,
       build(
         attrs
         |> Map.put(:quarantine_ref, quarantine_ref(attrs))
         |> Map.put(:quarantine_reason, inspect(reason))
         |> Map.put(:missing_fields, missing_fields),
         :diagnostic_quarantined
       )}
    end
  end

  defp validate_proof_match(attrs, proof) do
    mismatched =
      proof_join_fields(proof)
      |> Enum.filter(fn {field, proof_value} ->
        value = Map.get(attrs, field)
        ReleaseContractSupport.present_binary?(value) and value != proof_value
      end)
      |> Enum.map(fn {field, _proof_value} -> field end)

    case mismatched do
      [] -> :ok
      fields -> {:error, {:audit_restore_join_mismatch, fields}}
    end
  end

  defp put_proof_fields(attrs, proof) do
    Enum.reduce(proof_join_fields(proof), attrs, fn {field, value}, acc ->
      Map.put_new(acc, field, value)
    end)
  end

  defp proof_join_fields(%AuditInclusionProof{} = proof) do
    [
      installation_ref: proof.installation_id,
      trace_id: proof.trace_id,
      audit_fact_id: proof.audit_fact_id,
      checkpoint_ref: proof.checkpoint_ref,
      release_manifest_ref: proof.release_manifest_ref
    ]
  end

  defp quarantine_ref(attrs) do
    case Map.get(attrs, :quarantine_ref) do
      value when is_binary(value) and value != "" ->
        value

      _other ->
        "quarantine:restore-audit-join:#{Map.get(attrs, :restore_request_ref, "missing")}"
    end
  end
end

defmodule Mezzanine.Archival.ArchivalConflict do
  @moduledoc """
  Contract for deterministic hot/cold archival conflict quarantine.

  Contract: `Mezzanine.ArchivalConflict.v1`.
  """

  alias Mezzanine.Archival.ReleaseContractSupport

  @contract_name "Mezzanine.ArchivalConflict.v1"
  @precedence_rules [
    :hot_authoritative,
    :cold_authoritative,
    :quarantine_until_operator_resolution
  ]
  @required_binary_fields ReleaseContractSupport.base_required_binary_fields() ++
                            [
                              :conflict_ref,
                              :hot_hash,
                              :cold_hash,
                              :quarantine_ref,
                              :resolution_action_ref
                            ]
  @optional_binary_fields ReleaseContractSupport.optional_actor_fields() ++
                            [:retention_policy_ref, :cold_storage_uri_ref]

  defstruct [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :conflict_ref,
    :hot_hash,
    :cold_hash,
    :precedence_rule,
    :quarantine_ref,
    :resolution_action_ref,
    :retention_policy_ref,
    :cold_storage_uri_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_archival_conflict}
  def new(attrs) do
    with {:ok, attrs} <- ReleaseContractSupport.normalize_attrs(attrs),
         [] <- ReleaseContractSupport.missing_required_fields(attrs, @required_binary_fields, []),
         true <- ReleaseContractSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- ReleaseContractSupport.sha256?(Map.get(attrs, :hot_hash)),
         true <- ReleaseContractSupport.sha256?(Map.get(attrs, :cold_hash)),
         true <- Map.fetch!(attrs, :hot_hash) != Map.fetch!(attrs, :cold_hash),
         {:ok, precedence_rule} <-
           ReleaseContractSupport.enum_atom(Map.get(attrs, :precedence_rule), @precedence_rules) do
      {:ok, build(attrs, precedence_rule)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_archival_conflict}
    end
  end

  defp build(attrs, precedence_rule) do
    actors = ReleaseContractSupport.actor_fields(attrs)

    struct!(
      __MODULE__,
      Map.merge(
        attrs,
        Map.merge(actors, %{contract_name: @contract_name, precedence_rule: precedence_rule})
      )
    )
  end
end

defmodule Mezzanine.Archival.ArchivalSweep do
  @moduledoc """
  Contract for archival sweep retry and quarantine evidence.

  Contract: `Mezzanine.ArchivalSweep.v1`.
  """

  alias Mezzanine.Archival.ReleaseContractSupport

  @contract_name "Mezzanine.ArchivalSweep.v1"
  @required_binary_fields ReleaseContractSupport.base_required_binary_fields() ++
                            [
                              :sweep_ref,
                              :artifact_ref,
                              :retry_policy_ref,
                              :quarantine_ref
                            ]
  @optional_binary_fields ReleaseContractSupport.optional_actor_fields() ++
                            [:retention_policy_ref, :cold_storage_uri_ref]
  @required_datetime_fields [:next_retry_at]

  defstruct [
    :contract_name,
    :tenant_ref,
    :installation_ref,
    :workspace_ref,
    :project_ref,
    :environment_ref,
    :principal_ref,
    :system_actor_ref,
    :resource_ref,
    :authority_packet_ref,
    :permission_decision_ref,
    :idempotency_key,
    :trace_id,
    :correlation_id,
    :release_manifest_ref,
    :sweep_ref,
    :artifact_ref,
    :retry_count,
    :retry_policy_ref,
    :quarantine_ref,
    :next_retry_at,
    :retention_policy_ref,
    :cold_storage_uri_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_archival_sweep}
  def new(attrs) do
    with {:ok, attrs} <- ReleaseContractSupport.normalize_attrs(attrs),
         [] <-
           ReleaseContractSupport.missing_required_fields(
             attrs,
             @required_binary_fields,
             @required_datetime_fields
           ),
         true <- ReleaseContractSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- ReleaseContractSupport.non_neg_integer?(Map.get(attrs, :retry_count)) do
      {:ok, build(attrs)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_archival_sweep}
    end
  end

  defp build(attrs) do
    actors = ReleaseContractSupport.actor_fields(attrs)
    struct!(__MODULE__, Map.merge(attrs, Map.put(actors, :contract_name, @contract_name)))
  end
end
