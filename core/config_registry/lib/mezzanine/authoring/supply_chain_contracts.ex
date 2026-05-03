defmodule Mezzanine.Authoring.SupplyChainSupport do
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
    :release_manifest_ref,
    :pack_ref
  ]
  @optional_actor_fields [:principal_ref, :system_actor_ref]

  @spec base_required_binary_fields() :: [atom()]
  def base_required_binary_fields, do: @base_required_binary_fields

  @spec optional_actor_fields() :: [atom()]
  def optional_actor_fields, do: @optional_actor_fields

  @spec normalize_attrs(map() | keyword() | struct()) :: {:ok, map()} | {:error, :invalid_attrs}
  def normalize_attrs(attrs) when is_list(attrs), do: {:ok, Map.new(attrs)}

  def normalize_attrs(attrs) when is_map(attrs) do
    if Map.has_key?(attrs, :__struct__) do
      {:ok, Map.from_struct(attrs)}
    else
      {:ok, attrs}
    end
  end

  def normalize_attrs(_attrs), do: {:error, :invalid_attrs}

  @spec missing_required_fields(map(), [atom()], [atom()]) :: [atom()]
  def missing_required_fields(attrs, binary_fields, list_fields) do
    binary_missing =
      binary_fields
      |> Enum.reject(fn field -> present_binary?(Map.get(attrs, field)) end)

    list_missing =
      list_fields
      |> Enum.reject(fn field -> non_empty_binary_list?(Map.get(attrs, field)) end)

    actor_missing =
      if present_binary?(Map.get(attrs, :principal_ref)) or
           present_binary?(Map.get(attrs, :system_actor_ref)) do
        []
      else
        [:principal_ref_or_system_actor_ref]
      end

    binary_missing ++ actor_missing ++ list_missing
  end

  @spec optional_binary_fields?(map(), [atom()]) :: boolean()
  def optional_binary_fields?(attrs, fields) do
    Enum.all?(fields, fn field ->
      value = Map.get(attrs, field)
      is_nil(value) or present_binary?(value)
    end)
  end

  @spec present_binary?(term()) :: boolean()
  def present_binary?(value), do: is_binary(value) and byte_size(String.trim(value)) > 0

  @spec sha256?(term()) :: boolean()
  def sha256?(<<"sha256:", digest::binary-size(64)>>), do: lower_hex?(digest)
  def sha256?(_value), do: false

  defp lower_hex?(value) do
    value
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end

  @spec non_empty_binary_list?(term()) :: boolean()
  def non_empty_binary_list?(values) when is_list(values) do
    values != [] and Enum.all?(values, &present_binary?/1)
  end

  def non_empty_binary_list?(_values), do: false
end

defmodule Mezzanine.Authoring.ExtensionPackSignature do
  @moduledoc """
  Pack signature verification evidence for extension authoring/import.

  Contract: `Platform.ExtensionPackSignature.v1`.
  """

  alias Mezzanine.Authoring.SupplyChainSupport

  @contract_name "Platform.ExtensionPackSignature.v1"
  @algorithms ["hmac-sha256", "ed25519"]
  @required_binary_fields SupplyChainSupport.base_required_binary_fields() ++
                            [
                              :signature_ref,
                              :signing_key_ref,
                              :signature_algorithm,
                              :verification_hash,
                              :rejection_ref
                            ]
  @optional_binary_fields SupplyChainSupport.optional_actor_fields() ++
                            [:signing_key_rotation_ref]

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
    :pack_ref,
    :signature_ref,
    :signing_key_ref,
    :signature_algorithm,
    :verification_hash,
    :rejection_ref,
    :signing_key_rotation_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_extension_pack_signature}
  def new(attrs) do
    with {:ok, attrs} <- SupplyChainSupport.normalize_attrs(attrs),
         [] <- SupplyChainSupport.missing_required_fields(attrs, @required_binary_fields, []),
         true <- SupplyChainSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- Map.fetch!(attrs, :signature_algorithm) in @algorithms,
         true <- SupplyChainSupport.sha256?(Map.fetch!(attrs, :verification_hash)) do
      {:ok, struct!(__MODULE__, Map.put(attrs, :contract_name, @contract_name))}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_extension_pack_signature}
    end
  end
end

defmodule Mezzanine.Authoring.ExtensionPackBundle do
  @moduledoc """
  Pack bundle schema and declared-resource evidence for extension authoring/import.

  Contract: `Platform.ExtensionPackBundle.v1`.
  """

  alias Mezzanine.Authoring.SupplyChainSupport

  @contract_name "Platform.ExtensionPackBundle.v1"
  @required_binary_fields SupplyChainSupport.base_required_binary_fields() ++
                            [
                              :bundle_schema_version,
                              :schema_hash,
                              :validation_error_ref
                            ]
  @required_list_fields [:declared_resources]
  @optional_binary_fields SupplyChainSupport.optional_actor_fields() ++ [:capability_ref]

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
    :pack_ref,
    :bundle_schema_version,
    :declared_resources,
    :schema_hash,
    :validation_error_ref,
    :capability_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_extension_pack_bundle}
  def new(attrs) do
    with {:ok, attrs} <- SupplyChainSupport.normalize_attrs(attrs),
         [] <-
           SupplyChainSupport.missing_required_fields(
             attrs,
             @required_binary_fields,
             @required_list_fields
           ),
         true <- SupplyChainSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- SupplyChainSupport.sha256?(Map.fetch!(attrs, :schema_hash)) do
      {:ok, struct!(__MODULE__, Map.put(attrs, :contract_name, @contract_name))}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_extension_pack_bundle}
    end
  end
end
