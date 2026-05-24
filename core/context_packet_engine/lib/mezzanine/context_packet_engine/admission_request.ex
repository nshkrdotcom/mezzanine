defmodule Mezzanine.ContextPacketEngine.AdmissionRequest do
  @moduledoc """
  Ref-only request for admitting a compiled Context ABI packet into Mezzanine.
  """

  alias OuterBrain.ContextABI.Failure

  @required_fields [
    :tenant_ref,
    :workflow_ref,
    :authority_ref,
    :context_packet_ref,
    :idempotency_key,
    :trace_ref
  ]

  @optional_fields [
    :actor_ref,
    :ai_run_ref,
    :budget_ref,
    :cost_ref,
    :eval_ref,
    :route_decision_ref,
    :model_call_ref,
    :projection_ref,
    :metadata
  ]

  @fields @required_fields ++ @optional_fields

  @raw_keys MapSet.new(~w(
              access_token
              api_key
              authorization
              credential
              credential_material
              memory_body
              memory_content
              model_output
              password
              payload
              prompt
              prompt_body
              prompt_content
              prompt_text
              provider_payload
              provider_response
              raw
              raw_memory
              raw_payload
              raw_prompt
              raw_provider_payload
              refresh_token
              request_body
              response_body
              secret
              secret_token
              stderr
              stdout
              token
            ))

  @enforce_keys @required_fields
  defstruct @fields

  @type t :: %__MODULE__{
          tenant_ref: String.t(),
          workflow_ref: String.t(),
          authority_ref: String.t(),
          context_packet_ref: String.t(),
          idempotency_key: String.t(),
          trace_ref: String.t(),
          actor_ref: String.t() | nil,
          ai_run_ref: String.t() | nil,
          budget_ref: String.t() | nil,
          cost_ref: String.t() | nil,
          eval_ref: String.t() | nil,
          route_decision_ref: String.t() | nil,
          model_call_ref: String.t() | nil,
          projection_ref: String.t() | nil,
          metadata: map()
        }

  @spec new(t() | map() | keyword()) :: {:ok, t()} | {:error, Failure.t()}
  def new(%__MODULE__{} = request), do: request |> Map.from_struct() |> new()
  def new(attrs) when is_list(attrs), do: attrs |> Map.new() |> new()

  def new(attrs) when is_map(attrs) do
    with :ok <- reject_raw(attrs),
         {:ok, required} <- required_strings(attrs),
         {:ok, metadata} <- optional_map(attrs, :metadata) do
      {:ok,
       %__MODULE__{
         tenant_ref: required.tenant_ref,
         workflow_ref: required.workflow_ref,
         authority_ref: required.authority_ref,
         context_packet_ref: required.context_packet_ref,
         idempotency_key: required.idempotency_key,
         trace_ref: required.trace_ref,
         actor_ref: optional_string(attrs, :actor_ref),
         ai_run_ref: optional_string(attrs, :ai_run_ref),
         budget_ref: optional_string(attrs, :budget_ref),
         cost_ref: optional_string(attrs, :cost_ref),
         eval_ref: optional_string(attrs, :eval_ref),
         route_decision_ref: optional_string(attrs, :route_decision_ref),
         model_call_ref: optional_string(attrs, :model_call_ref),
         projection_ref: optional_string(attrs, :projection_ref),
         metadata: metadata
       }}
    end
  end

  def new(_attrs),
    do:
      failure("mezzanine.packet_admission.invalid_request.v1",
        safe_message: "packet admission request is invalid"
      )

  @spec joins(t()) :: map()
  def joins(%__MODULE__{} = request) do
    %{
      workflow_ref: request.workflow_ref,
      ai_run_ref: request.ai_run_ref,
      budget_ref: request.budget_ref,
      cost_ref: request.cost_ref,
      eval_ref: request.eval_ref,
      route_decision_ref: request.route_decision_ref,
      model_call_ref: request.model_call_ref,
      projection_ref: request.projection_ref,
      trace_ref: request.trace_ref
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp required_strings(attrs) do
    Enum.reduce_while(@required_fields, {:ok, %{}}, fn field, {:ok, acc} ->
      case string(attrs, field) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, field, value)}}
        {:error, _failure} = error -> {:halt, error}
      end
    end)
  end

  defp string(attrs, field) do
    case fetch(attrs, field) do
      value when is_binary(value) and value != "" ->
        {:ok, value}

      _other ->
        failure("mezzanine.packet_admission.missing_required_ref.v1",
          safe_message: "packet admission request is missing a required ref",
          evidence_refs: ["field://#{Atom.to_string(field)}"]
        )
    end
  end

  defp optional_string(attrs, field) do
    case fetch(attrs, field) do
      value when is_binary(value) and value != "" -> value
      _other -> nil
    end
  end

  defp optional_map(attrs, field) do
    case fetch(attrs, field, %{}) do
      value when is_map(value) ->
        {:ok, value}

      _other ->
        failure("mezzanine.packet_admission.invalid_metadata.v1",
          safe_message: "packet admission metadata is invalid"
        )
    end
  end

  defp reject_raw(attrs) do
    case find_raw_key(attrs) do
      nil ->
        :ok

      key ->
        failure("mezzanine.packet_admission.raw_payload_rejected.v1",
          safe_message: "packet admission does not accept raw payloads",
          evidence_refs: ["field://#{key}"]
        )
    end
  end

  defp find_raw_key(%{__struct__: _} = value), do: value |> Map.from_struct() |> find_raw_key()

  defp find_raw_key(%{} = map) do
    Enum.find_value(map, fn {key, value} ->
      key_string = key |> to_string() |> String.downcase()

      cond do
        MapSet.member?(@raw_keys, key_string) -> key_string
        String.starts_with?(key_string, "raw_") -> key_string
        true -> find_raw_key(value)
      end
    end)
  end

  defp find_raw_key(values) when is_list(values), do: Enum.find_value(values, &find_raw_key/1)
  defp find_raw_key(_value), do: nil

  defp fetch(attrs, field, default \\ nil)

  defp fetch(%{__struct__: _} = attrs, field, default),
    do: attrs |> Map.from_struct() |> fetch(field, default)

  defp fetch(attrs, field, default),
    do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field), default)

  defp failure(reason_code, opts) do
    {:ok, failure} =
      Failure.new(%{
        owner: :mezzanine,
        reason_code: reason_code,
        safe_message: Keyword.fetch!(opts, :safe_message),
        retryable?: Keyword.get(opts, :retryable?, false),
        trace_ref: Keyword.get(opts, :trace_ref),
        evidence_refs: Keyword.get(opts, :evidence_refs, [])
      })

    {:error, failure}
  end
end
