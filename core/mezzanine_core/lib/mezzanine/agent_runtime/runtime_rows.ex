defmodule Mezzanine.AgentRuntime.RuntimeEventRow do
  @moduledoc "Mechanism-neutral ordered runtime event row."

  alias Mezzanine.AgentRuntime.Support

  @required [
    :event_ref,
    :event_seq,
    :event_kind,
    :observed_at,
    :tenant_ref,
    :installation_ref,
    :subject_ref,
    :run_ref,
    :level,
    :message_summary
  ]
  @fields @required ++
            [
              :execution_ref,
              :workflow_ref,
              :attempt_ref,
              :session_ref,
              :turn_ref,
              :payload_ref,
              :extensions,
              :trace_id,
              :profile_ref,
              :source_contract_ref
            ]
  @enforce_keys @required
  @defaults @fields |> Enum.map(&{&1, nil}) |> Keyword.merge(extensions: %{})
  defstruct @defaults

  def new(attrs), do: build(__MODULE__, attrs, :invalid_runtime_event_row, @required)
  def new!(attrs), do: bang(new(attrs))
  def dump(%__MODULE__{} = row), do: dump_struct(row)

  defp build(module, attrs, error, required) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- Support.reject_unsafe(attrs, error),
         true <- Enum.all?(required, &present?(Support.required(attrs, &1))),
         extensions <- Support.optional(attrs, :extensions, %{}),
         true <- is_map(extensions) do
      values =
        module.__struct__()
        |> Map.from_struct()
        |> Map.new(fn {key, default} ->
          {key, Support.optional(attrs, key, default)}
        end)

      {:ok, struct!(module, Map.put(values, :extensions, extensions))}
    else
      _ -> {:error, error}
    end
  end

  defp present?(value) when is_integer(value), do: value >= 0
  defp present?(%DateTime{}), do: true
  defp present?(value), do: Support.safe_ref?(value)

  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))

  defp dump_struct(struct),
    do: struct |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()
end

defmodule Mezzanine.AgentRuntime.RuntimeCommandResult do
  @moduledoc "Shared runtime command result envelope."

  alias Mezzanine.AgentRuntime.Support

  @required [
    :command_ref,
    :command_kind,
    :status,
    :authority_state,
    :workflow_effect_state,
    :projection_state,
    :trace_id,
    :correlation_id,
    :idempotency_key,
    :message
  ]
  @fields @required ++ [:accepted?, :coalesced?, :authority_refs, :diagnostics]
  @enforce_keys @required
  @defaults %{
    command_ref: nil,
    command_kind: nil,
    status: nil,
    accepted?: false,
    coalesced?: false,
    authority_state: nil,
    authority_refs: [],
    workflow_effect_state: nil,
    projection_state: nil,
    trace_id: nil,
    correlation_id: nil,
    idempotency_key: nil,
    message: nil,
    diagnostics: []
  }
  defstruct command_ref: nil,
            command_kind: nil,
            status: nil,
            accepted?: false,
            coalesced?: false,
            authority_state: nil,
            authority_refs: [],
            workflow_effect_state: nil,
            projection_state: nil,
            trace_id: nil,
            correlation_id: nil,
            idempotency_key: nil,
            message: nil,
            diagnostics: []

  def new(attrs), do: build(attrs)
  def new!(attrs), do: bang(new(attrs))

  def dump(%__MODULE__{} = result),
    do: result |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()

  defp build(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- Support.reject_unsafe(attrs, :invalid_runtime_command_result),
         true <- Enum.all?(@required, &(Support.required(attrs, &1) |> present?())) do
      {:ok,
       struct!(
         __MODULE__,
         Map.new(@fields, &{&1, Support.optional(attrs, &1, Map.get(@defaults, &1))})
       )}
    else
      _ -> {:error, :invalid_runtime_command_result}
    end
  end

  defp present?(value), do: Support.safe_ref?(value) or is_atom(value)
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))
end

defmodule Mezzanine.AgentRuntime.RuntimeProjectionEnvelope do
  @moduledoc "S0 envelope for runtime projection payloads."

  alias Mezzanine.AgentRuntime.Support

  @required [
    :schema_ref,
    :schema_version,
    :projection_ref,
    :projection_name,
    :projection_kind,
    :tenant_ref,
    :installation_ref,
    :profile_ref,
    :scope_ref,
    :row_key,
    :updated_at,
    :computed_at,
    :payload
  ]
  @fields @required ++ [:staleness_ms, :trace_id, :causation_id, :diagnostics]
  @enforce_keys @required
  @defaults %{
    schema_ref: nil,
    schema_version: nil,
    projection_ref: nil,
    projection_name: nil,
    projection_kind: nil,
    tenant_ref: nil,
    installation_ref: nil,
    profile_ref: nil,
    scope_ref: nil,
    row_key: nil,
    updated_at: nil,
    computed_at: nil,
    staleness_ms: nil,
    trace_id: nil,
    causation_id: nil,
    payload: %{},
    diagnostics: []
  }
  defstruct schema_ref: nil,
            schema_version: nil,
            projection_ref: nil,
            projection_name: nil,
            projection_kind: nil,
            tenant_ref: nil,
            installation_ref: nil,
            profile_ref: nil,
            scope_ref: nil,
            row_key: nil,
            updated_at: nil,
            computed_at: nil,
            staleness_ms: nil,
            trace_id: nil,
            causation_id: nil,
            payload: %{},
            diagnostics: []

  def new(attrs) do
    with {:ok, attrs} <- Support.normalize_attrs(attrs),
         :ok <- Support.reject_unsafe(attrs, :invalid_runtime_projection_envelope),
         true <- Enum.all?(@required, &(Support.required(attrs, &1) |> present?())) do
      {:ok,
       struct!(
         __MODULE__,
         Map.new(@fields, &{&1, Support.optional(attrs, &1, Map.get(@defaults, &1))})
       )}
    else
      _ -> {:error, :invalid_runtime_projection_envelope}
    end
  end

  def new!(attrs), do: bang(new(attrs))

  def dump(%__MODULE__{} = envelope),
    do: envelope |> Map.from_struct() |> Support.dump_value() |> Support.drop_nil_values()

  defp present?(%DateTime{}), do: true
  defp present?(value) when is_integer(value), do: value >= 0
  defp present?(value) when is_map(value), do: true
  defp present?(value), do: Support.safe_ref?(value) or is_atom(value)
  defp bang({:ok, value}), do: value
  defp bang({:error, reason}), do: raise(ArgumentError, Atom.to_string(reason))
end
