defmodule Mezzanine.ControlRoom.ResourcePressureSupport do
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

  @spec missing_required_fields(map(), [atom()], [atom()], [atom()]) :: [atom()]
  def missing_required_fields(attrs, binary_fields, integer_fields, map_fields) do
    binary_missing =
      binary_fields
      |> Enum.reject(fn field -> present_binary?(Map.get(attrs, field)) end)

    integer_missing =
      integer_fields
      |> Enum.reject(fn field -> non_neg_integer?(Map.get(attrs, field)) end)

    map_missing =
      map_fields
      |> Enum.reject(fn field -> non_empty_map?(Map.get(attrs, field)) end)

    actor_missing =
      if present_binary?(Map.get(attrs, :principal_ref)) or
           present_binary?(Map.get(attrs, :system_actor_ref)) do
        []
      else
        [:principal_ref_or_system_actor_ref]
      end

    binary_missing ++ actor_missing ++ integer_missing ++ map_missing
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

  @spec non_neg_integer_fields?(map(), [atom()]) :: boolean()
  def non_neg_integer_fields?(attrs, fields) do
    Enum.all?(fields, fn field -> non_neg_integer?(Map.get(attrs, field)) end)
  end

  @spec non_empty_map?(term()) :: boolean()
  def non_empty_map?(value), do: is_map(value) and map_size(value) > 0

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

defmodule Mezzanine.ControlRoom.QueuePressurePolicy do
  @moduledoc """
  Queue pressure and deterministic shedding contract for Mezzanine retained queues.

  Contract: `Mezzanine.QueuePressurePolicy.v1`.
  """

  alias Mezzanine.ControlRoom.ResourcePressureSupport

  @contract_name "Mezzanine.QueuePressurePolicy.v1"
  @pressure_classes [:nominal, :soft_pressure, :hard_pressure, :queue_saturated]
  @shed_decisions [:accept, :throttle, :shed]
  @required_binary_fields ResourcePressureSupport.base_required_binary_fields() ++
                            [
                              :queue_name,
                              :queue_ref,
                              :budget_ref,
                              :pressure_sample_ref,
                              :threshold_ref,
                              :admission_decision_ref,
                              :shed_reason,
                              :operator_message_ref
                            ]
  @required_non_neg_integer_fields [:current_depth, :max_depth, :retry_after_ms]
  @optional_binary_fields ResourcePressureSupport.optional_actor_fields() ++ [:diagnostics_ref]

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
    :queue_name,
    :queue_ref,
    :budget_ref,
    :pressure_sample_ref,
    :threshold_ref,
    :pressure_class,
    :current_depth,
    :max_depth,
    :admission_decision_ref,
    :shed_decision,
    :shed_reason,
    :retry_after_ms,
    :operator_message_ref,
    :diagnostics_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_queue_pressure_policy}
  def new(attrs) do
    with {:ok, attrs} <- ResourcePressureSupport.normalize_attrs(attrs),
         [] <-
           ResourcePressureSupport.missing_required_fields(
             attrs,
             @required_binary_fields,
             [],
             []
           ),
         true <-
           ResourcePressureSupport.non_neg_integer_fields?(
             attrs,
             @required_non_neg_integer_fields
           ),
         true <- ResourcePressureSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         {:ok, pressure_class} <-
           ResourcePressureSupport.enum_atom(Map.get(attrs, :pressure_class), @pressure_classes),
         {:ok, shed_decision} <-
           ResourcePressureSupport.enum_atom(Map.get(attrs, :shed_decision), @shed_decisions),
         :ok <- validate_pressure_semantics(attrs, shed_decision) do
      {:ok, build(attrs, pressure_class, shed_decision)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_queue_pressure_policy}
    end
  end

  defp build(attrs, pressure_class, shed_decision) do
    actors = ResourcePressureSupport.actor_fields(attrs)

    struct!(
      __MODULE__,
      Map.merge(attrs, %{
        contract_name: @contract_name,
        principal_ref: actors.principal_ref,
        system_actor_ref: actors.system_actor_ref,
        pressure_class: pressure_class,
        shed_decision: shed_decision
      })
    )
  end

  defp validate_pressure_semantics(attrs, :shed) do
    cond do
      Map.fetch!(attrs, :current_depth) <= Map.fetch!(attrs, :max_depth) -> :error
      Map.fetch!(attrs, :retry_after_ms) <= 0 -> :error
      true -> :ok
    end
  end

  defp validate_pressure_semantics(attrs, :throttle) do
    if Map.fetch!(attrs, :retry_after_ms) > 0, do: :ok, else: :error
  end

  defp validate_pressure_semantics(_attrs, :accept), do: :ok
end

defmodule Mezzanine.ControlRoom.RetryPosture do
  @moduledoc """
  Platform retry posture contract for workflow, activity, and retained local-job operations.

  Contract: `Platform.RetryPosture.v1`.
  """

  alias Mezzanine.ControlRoom.ResourcePressureSupport

  @contract_name "Platform.RetryPosture.v1"
  @retry_classes [
    :never,
    :safe_idempotent,
    :after_input_change,
    :after_redecision,
    :manual_operator
  ]
  @required_binary_fields ResourcePressureSupport.base_required_binary_fields() ++
                            [
                              :operation_ref,
                              :owner_repo,
                              :producer_ref,
                              :consumer_ref,
                              :failure_class,
                              :idempotency_scope,
                              :dead_letter_ref,
                              :safe_action_code
                            ]
  @required_non_neg_integer_fields [:max_attempts]
  @required_map_fields [:backoff_policy]
  @optional_binary_fields ResourcePressureSupport.optional_actor_fields() ++
                            [:operator_message_ref]

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
    :operation_ref,
    :owner_repo,
    :producer_ref,
    :consumer_ref,
    :retry_class,
    :failure_class,
    :max_attempts,
    :backoff_policy,
    :idempotency_scope,
    :dead_letter_ref,
    :safe_action_code,
    :retry_after_ms,
    :operator_message_ref
  ]

  @type t :: %__MODULE__{}

  @spec contract_name() :: String.t()
  def contract_name, do: @contract_name

  @spec new(map() | keyword()) ::
          {:ok, t()}
          | {:error, {:missing_required_fields, [atom()]}}
          | {:error, :invalid_retry_posture}
  def new(attrs) do
    with {:ok, attrs} <- ResourcePressureSupport.normalize_attrs(attrs),
         [] <-
           ResourcePressureSupport.missing_required_fields(
             attrs,
             @required_binary_fields,
             [],
             @required_map_fields
           ),
         true <-
           ResourcePressureSupport.non_neg_integer_fields?(
             attrs,
             @required_non_neg_integer_fields
           ),
         true <- ResourcePressureSupport.optional_binary_fields?(attrs, @optional_binary_fields),
         true <- optional_non_neg_integer?(Map.get(attrs, :retry_after_ms)),
         {:ok, retry_class} <-
           ResourcePressureSupport.enum_atom(Map.get(attrs, :retry_class), @retry_classes),
         :ok <- validate_retry_semantics(attrs, retry_class) do
      {:ok, build(attrs, retry_class)}
    else
      fields when is_list(fields) -> {:error, {:missing_required_fields, fields}}
      _error -> {:error, :invalid_retry_posture}
    end
  end

  defp build(attrs, retry_class) do
    actors = ResourcePressureSupport.actor_fields(attrs)

    struct!(
      __MODULE__,
      Map.merge(attrs, %{
        contract_name: @contract_name,
        principal_ref: actors.principal_ref,
        system_actor_ref: actors.system_actor_ref,
        retry_class: retry_class
      })
    )
  end

  defp validate_retry_semantics(attrs, :never) do
    if Map.fetch!(attrs, :max_attempts) == 0, do: :ok, else: :error
  end

  defp validate_retry_semantics(attrs, _retry_class) do
    if Map.fetch!(attrs, :max_attempts) > 0, do: :ok, else: :error
  end

  defp optional_non_neg_integer?(nil), do: true
  defp optional_non_neg_integer?(value), do: ResourcePressureSupport.non_neg_integer?(value)
end
