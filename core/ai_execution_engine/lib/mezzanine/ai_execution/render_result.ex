defmodule Mezzanine.AIExecution.RenderResult do
  @moduledoc """
  Durable handoff from OuterBrain context rendering to model invocation.
  """

  alias OuterBrain.ContextABI.{ContextPacket, Failure}

  @required_fields [
    :tenant_ref,
    :workflow_ref,
    :context_packet_ref,
    :route_decision_ref,
    :prompt_artifact_ref,
    :provider_payload_ref,
    :payload_hash,
    :provider_family,
    :trace_ref
  ]

  @enforce_keys @required_fields
  defstruct @required_fields ++ [token_estimate: 0]

  @type t :: %__MODULE__{
          tenant_ref: String.t(),
          workflow_ref: String.t(),
          context_packet_ref: String.t(),
          route_decision_ref: String.t(),
          prompt_artifact_ref: String.t(),
          provider_payload_ref: String.t(),
          payload_hash: String.t(),
          provider_family: String.t(),
          token_estimate: non_neg_integer(),
          trace_ref: String.t()
        }

  @spec from_rendered(ContextPacket.t(), map(), map(), keyword()) ::
          {:ok, t()} | {:error, Failure.t()}
  def from_rendered(%ContextPacket{} = packet, route_decision, rendered, opts)
      when is_map(route_decision) and is_map(rendered) and is_list(opts) do
    attrs = %{
      tenant_ref: packet.tenant_ref,
      workflow_ref: Keyword.fetch!(opts, :workflow_ref),
      context_packet_ref: packet.context_packet_ref,
      route_decision_ref: get(route_decision, :route_decision_ref),
      prompt_artifact_ref: get(rendered, :prompt_artifact_ref),
      provider_payload_ref: get(rendered, :provider_payload_ref),
      payload_hash: get(rendered, :payload_hash),
      provider_family: get(rendered, :provider_family),
      token_estimate: get(rendered, :token_estimate, 0),
      trace_ref: get(rendered, :trace_ref) || packet.trace_ref
    }

    new(attrs)
  end

  @spec new(map()) :: {:ok, t()} | {:error, Failure.t()}
  def new(attrs) when is_map(attrs) do
    with :ok <- reject_raw(attrs),
         {:ok, required} <- required_strings(attrs),
         {:ok, token_estimate} <- non_negative_integer(attrs, :token_estimate) do
      {:ok,
       %__MODULE__{
         tenant_ref: required.tenant_ref,
         workflow_ref: required.workflow_ref,
         context_packet_ref: required.context_packet_ref,
         route_decision_ref: required.route_decision_ref,
         prompt_artifact_ref: required.prompt_artifact_ref,
         provider_payload_ref: required.provider_payload_ref,
         payload_hash: required.payload_hash,
         provider_family: required.provider_family,
         token_estimate: token_estimate,
         trace_ref: required.trace_ref
       }}
    end
  end

  def new(_attrs),
    do:
      failure("mezzanine.ai_execution.invalid_render_result.v1",
        safe_message: "render result is invalid"
      )

  defp required_strings(attrs) do
    Enum.reduce_while(@required_fields, {:ok, %{}}, fn field, {:ok, acc} ->
      case get(attrs, field) do
        value when is_binary(value) and value != "" ->
          {:cont, {:ok, Map.put(acc, field, value)}}

        _other ->
          error =
            failure("mezzanine.ai_execution.missing_render_ref.v1",
              safe_message: "render result is missing a required ref",
              evidence_refs: ["field://#{Atom.to_string(field)}"]
            )

          {:halt, error}
      end
    end)
  end

  defp non_negative_integer(attrs, field) do
    case get(attrs, field, 0) do
      value when is_integer(value) and value >= 0 ->
        {:ok, value}

      _other ->
        failure("mezzanine.ai_execution.invalid_token_estimate.v1",
          safe_message: "render result token estimate is invalid",
          evidence_refs: ["field://#{Atom.to_string(field)}"]
        )
    end
  end

  defp reject_raw(attrs) do
    forbidden = ~w(
      raw raw_prompt prompt prompt_text prompt_body provider_payload raw_provider_payload
      provider_response payload raw_payload request_body response_body credential api_key
      secret model_output
    )

    case find_raw_key(attrs, MapSet.new(forbidden)) do
      nil ->
        :ok

      key ->
        failure("mezzanine.ai_execution.raw_payload_rejected.v1",
          safe_message: "render result cannot carry raw payloads",
          evidence_refs: ["field://#{key}"]
        )
    end
  end

  defp find_raw_key(%{__struct__: _} = value, raw_keys),
    do: value |> Map.from_struct() |> find_raw_key(raw_keys)

  defp find_raw_key(%{} = map, raw_keys) do
    Enum.find_value(map, fn {key, value} ->
      key_string = key |> to_string() |> String.downcase()

      cond do
        MapSet.member?(raw_keys, key_string) -> key_string
        String.starts_with?(key_string, "raw_") -> key_string
        true -> find_raw_key(value, raw_keys)
      end
    end)
  end

  defp find_raw_key(values, raw_keys) when is_list(values),
    do: Enum.find_value(values, &find_raw_key(&1, raw_keys))

  defp find_raw_key(_value, _raw_keys), do: nil

  defp get(attrs, field, default \\ nil),
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
