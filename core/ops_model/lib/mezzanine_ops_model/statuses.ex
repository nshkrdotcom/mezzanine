defmodule MezzanineOpsModel.WorkStatus do
  @moduledoc "Canonical business-work lifecycle vocabulary."

  @values [
    :draft,
    :pending,
    :planning,
    :planned,
    :ready,
    :running,
    :blocked,
    :awaiting_review,
    :completed,
    :failed,
    :cancelled
  ]

  @type t ::
          :draft
          | :pending
          | :planning
          | :planned
          | :ready
          | :running
          | :blocked
          | :awaiting_review
          | :completed
          | :failed
          | :cancelled

  @spec values() :: [t()]
  def values, do: @values

  @spec valid?(term()) :: boolean()
  def valid?(value), do: match?({:ok, _}, cast(value))

  @spec cast(term()) :: {:ok, t()} | {:error, {:invalid_work_status, term()}}
  def cast(value) when value in @values, do: {:ok, value}
  def cast(:canceled), do: {:ok, :cancelled}
  def cast(value) when is_binary(value), do: cast_string(value)
  def cast(value), do: {:error, {:invalid_work_status, value}}

  @spec cast!(term()) :: t()
  def cast!(value) do
    case cast(value) do
      {:ok, cast_value} -> cast_value
      {:error, reason} -> raise ArgumentError, "invalid work status: #{inspect(reason)}"
    end
  end

  defp cast_string(value) do
    case String.trim(value) do
      "canceled" -> {:ok, :cancelled}
      "cancelled" -> {:ok, :cancelled}
      candidate -> cast_existing_atom(candidate)
    end
  end

  defp cast_existing_atom(value) do
    value
    |> String.to_existing_atom()
    |> cast()
  rescue
    ArgumentError -> {:error, {:invalid_work_status, value}}
  end
end

defmodule MezzanineOpsModel.RunStatus do
  @moduledoc "Canonical run lifecycle vocabulary."

  @values [:pending, :scheduled, :running, :completed, :failed, :cancelled, :stalled]

  @type t :: :pending | :scheduled | :running | :completed | :failed | :cancelled | :stalled

  @spec values() :: [t()]
  def values, do: @values

  @spec cast(term()) :: {:ok, t()} | {:error, {:invalid_run_status, term()}}
  def cast(value) when value in @values, do: {:ok, value}
  def cast(:canceled), do: {:ok, :cancelled}

  def cast(value) when is_binary(value) do
    case String.trim(value) do
      "canceled" -> {:ok, :cancelled}
      "cancelled" -> {:ok, :cancelled}
      candidate -> cast_existing_atom(candidate)
    end
  end

  def cast(value), do: {:error, {:invalid_run_status, value}}

  @spec cast!(term()) :: t()
  def cast!(value) do
    case cast(value) do
      {:ok, cast_value} -> cast_value
      {:error, reason} -> raise ArgumentError, "invalid run status: #{inspect(reason)}"
    end
  end

  defp cast_existing_atom(value) do
    value
    |> String.to_existing_atom()
    |> cast()
  rescue
    ArgumentError -> {:error, {:invalid_run_status, value}}
  end
end

defmodule MezzanineOpsModel.ReviewStatus do
  @moduledoc "Canonical review lifecycle vocabulary."

  @values [:pending, :in_review, :accepted, :rejected, :waived, :escalated]

  @type t :: :pending | :in_review | :accepted | :rejected | :waived | :escalated

  @spec values() :: [t()]
  def values, do: @values

  @spec cast(term()) :: {:ok, t()} | {:error, {:invalid_review_status, term()}}
  def cast(value) when value in @values, do: {:ok, value}
  def cast(:approved), do: {:ok, :accepted}

  def cast(value) when is_binary(value) do
    case String.trim(value) do
      "approved" -> {:ok, :accepted}
      "accepted" -> {:ok, :accepted}
      candidate -> cast_existing_atom(candidate)
    end
  end

  def cast(value), do: {:error, {:invalid_review_status, value}}

  @spec cast!(term()) :: t()
  def cast!(value) do
    case cast(value) do
      {:ok, cast_value} -> cast_value
      {:error, reason} -> raise ArgumentError, "invalid review status: #{inspect(reason)}"
    end
  end

  defp cast_existing_atom(value) do
    value
    |> String.to_existing_atom()
    |> cast()
  rescue
    ArgumentError -> {:error, {:invalid_review_status, value}}
  end
end

defmodule MezzanineOpsModel.AuditEventKind do
  @moduledoc "Stable audit vocabulary for higher-order operational events."

  @values [
    :work_ingested,
    :work_planned,
    :work_blocked,
    :work_completed,
    :run_scheduled,
    :run_started,
    :run_completed,
    :run_failed,
    :review_created,
    :review_accepted,
    :review_rejected,
    :review_waived,
    :escalation_raised,
    :escalation_resolved,
    :operator_paused,
    :operator_resumed,
    :operator_cancelled,
    :grant_override_applied,
    :replan_requested
  ]

  @type t ::
          :work_ingested
          | :work_planned
          | :work_blocked
          | :work_completed
          | :run_scheduled
          | :run_started
          | :run_completed
          | :run_failed
          | :review_created
          | :review_accepted
          | :review_rejected
          | :review_waived
          | :escalation_raised
          | :escalation_resolved
          | :operator_paused
          | :operator_resumed
          | :operator_cancelled
          | :grant_override_applied
          | :replan_requested

  @spec values() :: [t()]
  def values, do: @values

  @spec cast(term()) :: {:ok, t()} | {:error, {:invalid_audit_event_kind, term()}}
  def cast(value) when value in @values, do: {:ok, value}

  def cast(value) when is_binary(value) do
    case String.trim(value) do
      "work_created" -> {:ok, :work_ingested}
      "plan_compiled" -> {:ok, :work_planned}
      "run_requested" -> {:ok, :run_scheduled}
      "review_decided" -> {:ok, :review_accepted}
      candidate -> cast_existing_atom(candidate)
    end
  end

  def cast(value), do: {:error, {:invalid_audit_event_kind, value}}

  @spec cast!(term()) :: t()
  def cast!(value) do
    case cast(value) do
      {:ok, cast_value} -> cast_value
      {:error, reason} -> raise ArgumentError, "invalid audit event kind: #{inspect(reason)}"
    end
  end

  defp cast_existing_atom(value) do
    value
    |> String.to_existing_atom()
    |> cast()
  rescue
    ArgumentError -> {:error, {:invalid_audit_event_kind, value}}
  end
end
