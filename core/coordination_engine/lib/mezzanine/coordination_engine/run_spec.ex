defmodule Mezzanine.CoordinationEngine.RunSpec do
  @moduledoc """
  Ref-only governed TRINITY coordination run specification.
  """

  alias Mezzanine.CoordinationEngine.Validation

  @required_strings [
    :coordination_run_ref,
    :tenant_ref,
    :authority_ref,
    :actor_ref,
    :subject_ref,
    :persistence_profile_ref,
    :router_session_ref,
    :router_config_ref,
    :provider_pool_ref,
    :role_registry_ref,
    :replay_ref,
    :cost_budget_ref,
    :context_budget_ref,
    :operation_policy_ref
  ]

  @required_lists [
    :memory_ref_set,
    :prompt_ref_set,
    :model_profile_ref_set,
    :target_ref_set,
    :trace_ref_set
  ]

  @enforce_keys @required_strings ++ @required_lists
  defstruct @enforce_keys ++ [:cancellation_ref, :retry_ref]

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- Validation.reject_raw(attrs),
         {:ok, strings} <- required_strings(attrs),
         {:ok, lists} <- required_lists(attrs) do
      spec =
        strings
        |> Map.merge(lists)
        |> Map.put(:cancellation_ref, Validation.fetch(attrs, :cancellation_ref))
        |> Map.put(:retry_ref, Validation.fetch(attrs, :retry_ref))

      {:ok, struct!(__MODULE__, spec)}
    end
  end

  def new(_attrs), do: {:error, :invalid_coordination_run_spec}

  @spec new!(map()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, spec} -> spec
      {:error, reason} -> raise ArgumentError, "invalid coordination run spec: #{inspect(reason)}"
    end
  end

  @spec to_ai_run_attrs(t()) :: map()
  def to_ai_run_attrs(%__MODULE__{} = spec) do
    %{
      ai_run_ref: spec.coordination_run_ref,
      run_class: :coordination_run,
      tenant_ref: spec.tenant_ref,
      subject_ref: spec.subject_ref,
      authority_ref: spec.authority_ref,
      actor_ref: spec.actor_ref,
      persistence_profile_ref: %{
        id: :mickey_mouse,
        store_ref: spec.persistence_profile_ref,
        selected_tier: :memory_ephemeral
      },
      memory_ref_set: spec.memory_ref_set,
      prompt_ref_set: spec.prompt_ref_set,
      model_profile_ref_set: spec.model_profile_ref_set,
      operation_policy_ref: spec.operation_policy_ref,
      target_ref_set: spec.target_ref_set,
      trace_ref_set: spec.trace_ref_set,
      budget_ref_set: [spec.cost_budget_ref, spec.context_budget_ref],
      idempotency_ref: "idempotency:" <> spec.coordination_run_ref,
      cancellation_ref: spec.cancellation_ref,
      retry_ref: spec.retry_ref,
      lifecycle_state: :created
    }
  end

  defp required_strings(attrs) do
    Enum.reduce_while(@required_strings, {:ok, %{}}, fn key, {:ok, acc} ->
      case Validation.require_binary(attrs, key) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp required_lists(attrs) do
    Enum.reduce_while(@required_lists, {:ok, %{}}, fn key, {:ok, acc} ->
      case Validation.require_string_list(attrs, key) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, key, value)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
