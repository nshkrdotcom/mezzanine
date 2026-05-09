defmodule Mezzanine.WorkflowRuntime.WorkflowRetryEvent do
  @moduledoc """
  Pure workflow-runtime retry event contract.

  The events are intentionally small. They give WorkControl and the operator
  surface stable facts for retry/readback while workflow execution remains owned
  by WorkflowRuntime.
  """

  @derive Jason.Encoder
  @enforce_keys [
    :event_id,
    :event_kind,
    :workflow_id,
    :workflow_version,
    :attempt,
    :idempotency_key,
    :retry_token,
    :retry_class,
    :safe_action,
    :terminal?,
    :allowed?,
    :occurred_at
  ]
  defstruct [
    :event_id,
    :event_kind,
    :workflow_id,
    :workflow_run_id,
    :workflow_type,
    :workflow_version,
    :attempt,
    :retry_slot,
    :max_retry_slots,
    :idempotency_key,
    :retry_token,
    :retry_class,
    :safe_action,
    :reason,
    :backoff_ms,
    :denial_class,
    :terminal?,
    :allowed?,
    :occurred_at,
    metadata: %{}
  ]

  @type event_kind ::
          :normal_continuation_retry
          | :abnormal_backoff_retry
          | :retry_slot_exhausted
          | :terminal_retry_denial

  @type t :: %__MODULE__{}

  @event_names %{
    normal_continuation_retry: "workflow.retry.continuation",
    abnormal_backoff_retry: "workflow.retry.backoff",
    retry_slot_exhausted: "workflow.retry.exhausted",
    terminal_retry_denial: "workflow.retry.denied"
  }
  @struct_fields [
    :event_id,
    :event_kind,
    :workflow_id,
    :workflow_run_id,
    :workflow_type,
    :workflow_version,
    :attempt,
    :retry_slot,
    :max_retry_slots,
    :idempotency_key,
    :retry_token,
    :retry_class,
    :safe_action,
    :reason,
    :backoff_ms,
    :denial_class,
    :terminal?,
    :allowed?,
    :occurred_at,
    :metadata
  ]
  @key_lookup Map.new(@struct_fields, &{Atom.to_string(&1), &1})

  @spec event_kinds() :: %{event_kind() => String.t()}
  def event_kinds, do: @event_names

  @spec normal_continuation_retry(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def normal_continuation_retry(attrs) do
    attrs
    |> normalize()
    |> Map.merge(%{
      event_kind: :normal_continuation_retry,
      retry_class: "normal_continuation",
      safe_action: "retry_now",
      terminal?: false,
      allowed?: true,
      backoff_ms: 0
    })
    |> new()
  end

  @spec abnormal_backoff_retry(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def abnormal_backoff_retry(attrs) do
    attrs
    |> normalize()
    |> Map.merge(%{
      event_kind: :abnormal_backoff_retry,
      retry_class: "abnormal_backoff",
      safe_action: "retry_after_backoff",
      terminal?: false,
      allowed?: true,
      backoff_ms: map_value(attrs, :backoff_ms) || 5_000
    })
    |> new()
  end

  @spec retry_slot_exhausted(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def retry_slot_exhausted(attrs) do
    attrs
    |> normalize()
    |> Map.merge(%{
      event_kind: :retry_slot_exhausted,
      retry_class: "retry_slot_exhausted",
      safe_action: "surface_to_operator",
      terminal?: true,
      allowed?: false,
      backoff_ms: nil,
      denial_class: "retry_budget_exhausted"
    })
    |> new()
  end

  @spec terminal_retry_denial(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def terminal_retry_denial(attrs) do
    attrs
    |> normalize()
    |> Map.merge(%{
      event_kind: :terminal_retry_denial,
      retry_class: "terminal_retry_denial",
      safe_action: "deny_retry",
      terminal?: true,
      allowed?: false,
      backoff_ms: nil,
      denial_class: map_value(attrs, :denial_class) || "terminal_retry_denied"
    })
    |> new()
  end

  @spec stale?(t(), map() | keyword()) :: boolean()
  def stale?(%__MODULE__{} = event, current) do
    current = normalize(current)

    Enum.any?([:workflow_version, :attempt, :idempotency_key], fn key ->
      Map.fetch!(Map.from_struct(event), key) != Map.get(current, key)
    end)
  end

  @spec guard_retry_token(t(), map() | keyword()) :: :ok | {:error, term()}
  def guard_retry_token(%__MODULE__{} = event, current) do
    current = normalize(current)

    cond do
      stale?(event, current) ->
        {:error,
         {:stale_retry_token,
          %{
            expected: retry_token(event),
            got: retry_token(current)
          }}}

      retry_token(event) != retry_token(current) ->
        {:error,
         {:retry_token_mismatch,
          %{
            expected: retry_token(event),
            got: retry_token(current)
          }}}

      true ->
        :ok
    end
  end

  @spec retry_token(t() | map() | keyword()) :: map()
  def retry_token(%__MODULE__{} = event), do: event |> Map.from_struct() |> retry_token()

  def retry_token(attrs) do
    attrs = normalize(attrs)

    %{
      workflow_id: Map.get(attrs, :workflow_id),
      workflow_version: Map.get(attrs, :workflow_version),
      attempt: Map.get(attrs, :attempt),
      retry_slot: Map.get(attrs, :retry_slot),
      idempotency_key_hash: hash(Map.get(attrs, :idempotency_key))
    }
  end

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, term()}
  def new(attrs) do
    attrs = normalize(attrs)

    with {:ok, event_kind} <- event_kind(attrs),
         {:ok, workflow_id} <- required_string(attrs, :workflow_id),
         {:ok, workflow_version} <- required_string(attrs, :workflow_version),
         {:ok, attempt} <- required_integer(attrs, :attempt),
         {:ok, idempotency_key} <- required_string(attrs, :idempotency_key) do
      attrs =
        attrs
        |> Map.put(:event_kind, event_kind)
        |> Map.put(:workflow_id, workflow_id)
        |> Map.put(:workflow_version, workflow_version)
        |> Map.put(:attempt, attempt)
        |> Map.put(:idempotency_key, idempotency_key)
        |> Map.put_new(:occurred_at, DateTime.utc_now() |> DateTime.truncate(:microsecond))
        |> Map.put_new(:metadata, %{})

      attrs =
        attrs
        |> Map.put_new(:event_id, event_id(attrs))
        |> Map.put_new(:retry_token, retry_token(attrs))

      {:ok, struct!(__MODULE__, Map.take(attrs, @struct_fields))}
    end
  end

  defp event_kind(attrs) do
    case Map.get(attrs, :event_kind) do
      kind when is_atom(kind) and is_map_key(@event_names, kind) -> {:ok, kind}
      kind when is_binary(kind) -> event_kind_from_name(kind)
      other -> {:error, {:unknown_retry_event_kind, other}}
    end
  end

  defp event_kind_from_name(name) do
    case Enum.find(@event_names, fn {_kind, event_name} -> event_name == name end) do
      {kind, _name} -> {:ok, kind}
      nil -> {:error, {:unknown_retry_event_kind, name}}
    end
  end

  defp event_id(attrs) do
    kind = Map.fetch!(attrs, :event_kind)

    [
      Map.get(@event_names, kind, Atom.to_string(kind)),
      Map.get(attrs, :workflow_id),
      Map.get(attrs, :workflow_version),
      Map.get(attrs, :attempt),
      Map.get(attrs, :retry_slot),
      hash(Map.get(attrs, :idempotency_key))
    ]
    |> Enum.map_join(":", &to_string/1)
  end

  defp required_string(attrs, key) do
    case Map.get(attrs, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, {:missing_required_retry_event_field, key}}
    end
  end

  defp required_integer(attrs, key) do
    case Map.get(attrs, key) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      _other -> {:error, {:missing_required_retry_event_field, key}}
    end
  end

  defp normalize(attrs) when is_list(attrs), do: attrs |> Map.new() |> normalize()
  defp normalize(%_{} = struct), do: struct |> Map.from_struct() |> normalize()

  defp normalize(attrs) when is_map(attrs) do
    Map.new(attrs, fn
      {key, value} when is_binary(key) -> {Map.get(@key_lookup, key, key), value}
      pair -> pair
    end)
  end

  defp map_value(attrs, key), do: attrs |> normalize() |> Map.get(key)

  defp hash(nil), do: nil

  defp hash(value) do
    value
    |> to_string()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
