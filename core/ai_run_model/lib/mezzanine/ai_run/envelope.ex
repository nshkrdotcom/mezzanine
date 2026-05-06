defmodule Mezzanine.AIRun.Envelope do
  @moduledoc """
  Ref-only AI run envelope.
  """

  alias Mezzanine.AIRun.{Lifecycle, PersistenceRefs, RunClass}

  @required_refs [:ai_run_ref, :tenant_ref, :authority_ref, :actor_ref]

  @ref_set_fields [
    :memory_ref_set,
    :prompt_ref_set,
    :skill_ref_set,
    :model_profile_ref_set,
    :target_ref_set,
    :trace_ref_set,
    :eval_ref_set,
    :budget_ref_set,
    :promotion_ref_set
  ]

  @forbidden_raw_keys [
    :raw_credentials,
    :raw_prompt,
    :raw_provider_payload,
    :raw_model_output,
    :raw_tool_input,
    :raw_tool_output,
    :raw_auth_material,
    :api_key,
    :authorization_header,
    :oauth_secret,
    :native_auth_file_contents,
    :token_file,
    :credential_body,
    :memory_body,
    :operator_private_payload
  ]

  @enforce_keys [
    :ai_run_ref,
    :run_class,
    :tenant_ref,
    :authority_ref,
    :actor_ref,
    :persistence_profile_ref,
    :lifecycle_state
  ]
  defstruct [
              :parent_run_ref,
              :subject_ref,
              :operation_policy_ref,
              :idempotency_ref,
              :cancellation_ref,
              :retry_ref,
              :supersession_ref,
              :rollback_ref,
              metadata: %{}
            ] ++ @enforce_keys ++ Enum.map(@ref_set_fields, &{&1, []})

  @type t :: %__MODULE__{}

  @spec new(map()) :: {:ok, t()} | {:error, term()}
  def new(attrs) when is_map(attrs) do
    with :ok <- validate_required_refs(attrs),
         {:ok, run_class} <- run_class(attrs),
         {:ok, lifecycle_state} <- lifecycle_state(attrs),
         {:ok, persistence_profile_ref} <-
           PersistenceRefs.resolve(get(attrs, :persistence_profile_ref)),
         :ok <- reject_raw(attrs) do
      {:ok,
       %__MODULE__{
         ai_run_ref: get(attrs, :ai_run_ref),
         run_class: run_class,
         parent_run_ref: get(attrs, :parent_run_ref),
         tenant_ref: get(attrs, :tenant_ref),
         subject_ref: get(attrs, :subject_ref),
         authority_ref: get(attrs, :authority_ref),
         actor_ref: get(attrs, :actor_ref),
         persistence_profile_ref: persistence_profile_ref,
         memory_ref_set: ref_set(attrs, :memory_ref_set),
         prompt_ref_set: ref_set(attrs, :prompt_ref_set),
         skill_ref_set: ref_set(attrs, :skill_ref_set),
         model_profile_ref_set: ref_set(attrs, :model_profile_ref_set),
         operation_policy_ref: get(attrs, :operation_policy_ref),
         target_ref_set: ref_set(attrs, :target_ref_set),
         trace_ref_set: ref_set(attrs, :trace_ref_set),
         eval_ref_set: ref_set(attrs, :eval_ref_set),
         budget_ref_set: ref_set(attrs, :budget_ref_set),
         promotion_ref_set: ref_set(attrs, :promotion_ref_set),
         lifecycle_state: lifecycle_state,
         idempotency_ref: get(attrs, :idempotency_ref),
         cancellation_ref: get(attrs, :cancellation_ref),
         retry_ref: get(attrs, :retry_ref),
         supersession_ref: get(attrs, :supersession_ref),
         rollback_ref: get(attrs, :rollback_ref),
         metadata: get(attrs, :metadata) || %{}
       }}
    end
  end

  def new(_attrs), do: {:error, :invalid_ai_run_envelope}

  @spec new!(map()) :: t()
  def new!(attrs) do
    case new(attrs) do
      {:ok, envelope} -> envelope
      {:error, reason} -> raise ArgumentError, "invalid AI run envelope: #{inspect(reason)}"
    end
  end

  @spec redacted_projection(t()) :: t()
  def redacted_projection(%__MODULE__{} = envelope), do: envelope

  defp validate_required_refs(attrs) do
    case Enum.find(@required_refs, &(required_ref?(attrs, &1) == false)) do
      nil -> :ok
      field -> {:error, {:missing_ref, field}}
    end
  end

  defp required_ref?(attrs, field) do
    case get(attrs, field) do
      value when is_binary(value) -> String.trim(value) != ""
      value when is_map(value) -> map_size(value) > 0
      _other -> false
    end
  end

  defp run_class(attrs) do
    class = get(attrs, :run_class)

    if RunClass.valid?(class), do: {:ok, class}, else: {:error, {:invalid_run_class, class}}
  end

  defp lifecycle_state(attrs) do
    state = get(attrs, :lifecycle_state) || Lifecycle.initial_state()

    if Lifecycle.valid?(state),
      do: {:ok, state},
      else: {:error, {:invalid_lifecycle_state, state}}
  end

  defp ref_set(attrs, field) do
    case get(attrs, field) do
      nil -> []
      value when is_list(value) -> value
      value -> [value]
    end
  end

  defp reject_raw(attrs) do
    case raw_path(attrs, []) do
      nil -> :ok
      path -> {:error, {:raw_projection_forbidden, path}}
    end
  end

  defp raw_path(%struct{} = value, path) when is_atom(struct) do
    value
    |> Map.from_struct()
    |> raw_path(path)
  end

  defp raw_path(map, path) when is_map(map) do
    Enum.reduce_while(map, nil, fn {key, value}, _acc ->
      normalized_key = normalize_key(key)
      next_path = path ++ [normalized_key]

      cond do
        normalized_key in @forbidden_raw_keys ->
          {:halt, next_path}

        found = raw_path(value, next_path) ->
          {:halt, found}

        true ->
          {:cont, nil}
      end
    end)
  end

  defp raw_path(values, path) when is_list(values) do
    Enum.reduce_while(values, nil, fn value, _acc ->
      case raw_path(value, path) do
        nil -> {:cont, nil}
        found -> {:halt, found}
      end
    end)
  end

  defp raw_path(_value, _path), do: nil

  defp normalize_key(key) when is_atom(key), do: key
  defp normalize_key(key) when is_binary(key), do: normalize_binary_key(key)
  defp normalize_key(key), do: key

  defp normalize_binary_key("raw_credentials"), do: :raw_credentials
  defp normalize_binary_key("raw_prompt"), do: :raw_prompt
  defp normalize_binary_key("raw_provider_payload"), do: :raw_provider_payload
  defp normalize_binary_key("raw_model_output"), do: :raw_model_output
  defp normalize_binary_key("raw_tool_input"), do: :raw_tool_input
  defp normalize_binary_key("raw_tool_output"), do: :raw_tool_output
  defp normalize_binary_key("raw_auth_material"), do: :raw_auth_material
  defp normalize_binary_key("api_key"), do: :api_key
  defp normalize_binary_key("authorization_header"), do: :authorization_header
  defp normalize_binary_key("oauth_secret"), do: :oauth_secret
  defp normalize_binary_key("native_auth_file_contents"), do: :native_auth_file_contents
  defp normalize_binary_key("token_file"), do: :token_file
  defp normalize_binary_key("credential_body"), do: :credential_body
  defp normalize_binary_key("memory_body"), do: :memory_body
  defp normalize_binary_key("operator_private_payload"), do: :operator_private_payload
  defp normalize_binary_key(key), do: key

  defp get(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end
end
