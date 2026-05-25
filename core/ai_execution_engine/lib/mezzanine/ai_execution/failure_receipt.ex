defmodule Mezzanine.AIExecution.FailureReceipt do
  @moduledoc """
  Durable, ref-only receipt for failures crossing the AI execution lifecycle.
  """

  alias GroundPlane.Boundary.Codec
  alias OuterBrain.ContextABI.Failure

  @stages [
    :context,
    :authority,
    :router,
    :render,
    :model_invocation,
    :eval,
    :memory,
    :optimization,
    :promotion,
    :evidence
  ]
  @raw_keys MapSet.new(~w(
              body
              credential
              credential_material
              eval_output
              eval_payload
              memory_body
              model_output
              payload
              prompt
              prompt_body
              provider_payload
              provider_response
              raw
              raw_body
              raw_memory
              raw_payload
              raw_prompt
              secret
              token
            ))

  @required_fields [
    :failure_receipt_ref,
    :failure_ref,
    :tenant_ref,
    :workflow_ref,
    :stage,
    :owner,
    :reason_code,
    :failure_family,
    :safe_message,
    :product_summary,
    :operator_summary,
    :safe_action,
    :status,
    :retryable?,
    :trace_ref,
    :evidence_refs
  ]
  @enforce_keys @required_fields
  defstruct @required_fields

  @type t :: %__MODULE__{}

  @spec new(Failure.t(), map() | keyword()) :: {:ok, t()} | {:error, Failure.t()}
  def new(failure, attrs \\ %{})

  def new(%Failure{} = failure, attrs) do
    attrs = if is_list(attrs), do: Map.new(attrs), else: attrs

    with :ok <- reject_raw(attrs),
         {:ok, summary} <- Failure.summary(failure),
         {:ok, tenant_ref} <- required_string(attrs, :tenant_ref),
         {:ok, workflow_ref} <- required_string(attrs, :workflow_ref),
         {:ok, trace_ref} <- trace_ref(attrs, failure),
         {:ok, stage} <- stage(attrs, summary.failure_family) do
      {:ok,
       %__MODULE__{
         failure_receipt_ref: receipt_ref(summary.failure_ref, tenant_ref, workflow_ref, stage),
         failure_ref: summary.failure_ref,
         tenant_ref: tenant_ref,
         workflow_ref: workflow_ref,
         stage: stage,
         owner: summary.owner,
         reason_code: summary.reason_code,
         failure_family: summary.failure_family,
         safe_message: summary.safe_message,
         product_summary: summary.product_summary,
         operator_summary: summary.operator_summary,
         safe_action: summary.safe_action,
         status: :failed,
         retryable?: summary.retryable?,
         trace_ref: trace_ref,
         evidence_refs: summary.evidence_refs
       }}
    else
      {:error, %Failure{} = reason} -> {:error, reason}
      {:error, reason} -> failure(reason)
    end
  end

  def new(_failure, _attrs), do: failure(:invalid_failure_receipt)

  defp trace_ref(attrs, failure) do
    case value(attrs, :trace_ref) || failure.trace_ref do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> failure(:missing_trace_ref)
    end
  end

  defp stage(attrs, default_family) do
    candidate = value(attrs, :stage) || default_stage(default_family)

    cond do
      candidate in @stages ->
        {:ok, candidate}

      is_binary(candidate) ->
        Enum.find(@stages, &(Atom.to_string(&1) == candidate))
        |> case do
          nil -> failure(:invalid_stage)
          found -> {:ok, found}
        end

      true ->
        failure(:invalid_stage)
    end
  end

  defp default_stage(:model_execution), do: :model_invocation
  defp default_stage(family), do: family

  defp required_string(attrs, field) do
    case value(attrs, field) do
      candidate when is_binary(candidate) and candidate != "" -> {:ok, candidate}
      _other -> failure({:missing_ref, field})
    end
  end

  defp reject_raw(attrs) do
    case raw_key(attrs) do
      nil -> :ok
      key -> failure({:raw_field, key})
    end
  end

  defp raw_key(%{__struct__: _} = value), do: value |> Map.from_struct() |> raw_key()

  defp raw_key(value) when is_map(value) do
    Enum.find_value(value, fn {key, nested} ->
      key_string = key |> to_string() |> String.downcase()

      cond do
        MapSet.member?(@raw_keys, key_string) -> key_string
        String.starts_with?(key_string, "raw_") -> key_string
        true -> raw_key(nested)
      end
    end)
  end

  defp raw_key(values) when is_list(values), do: Enum.find_value(values, &raw_key/1)
  defp raw_key(_value), do: nil

  defp receipt_ref(failure_ref, tenant_ref, workflow_ref, stage) do
    digest =
      %{
        failure_ref: failure_ref,
        tenant_ref: tenant_ref,
        workflow_ref: workflow_ref,
        stage: Atom.to_string(stage)
      }
      |> Codec.digest()
      |> String.replace_prefix("sha256:", "")

    "ai-execution-failure-receipt://#{digest}"
  end

  defp value(attrs, field), do: Map.get(attrs, field) || Map.get(attrs, Atom.to_string(field))

  defp failure(reason) do
    safe_message =
      case reason do
        {:raw_field, _field} -> "AI execution failure receipt cannot carry raw payloads"
        _other -> "AI execution failure receipt is invalid"
      end

    evidence_refs =
      case reason do
        {:missing_ref, field} -> ["field://#{Atom.to_string(field)}"]
        {:raw_field, field} -> ["field://#{field}"]
        _other -> ["reason://#{inspect(reason)}"]
      end

    {:ok, failure} =
      Failure.new(%{
        owner: :mezzanine,
        reason_code: "mezzanine.ai_execution.failure_receipt_invalid.v1",
        safe_message: safe_message,
        evidence_refs: evidence_refs
      })

    {:error, failure}
  end
end
