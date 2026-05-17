defmodule Mezzanine.Substrate.Transitions do
  @moduledoc "Pure state transitions for substrate records."

  alias Mezzanine.Substrate.OperationRequest
  alias Mezzanine.Substrate.ReviewCase
  alias Mezzanine.Substrate.SourceItem
  alias Mezzanine.Substrate.WorkflowRun

  @source_transitions %{
    admitted: [:deduplicated, :publication_requested, :rejected],
    deduplicated: [:publication_requested, :rejected],
    publication_requested: [:published, :rejected],
    published: [],
    rejected: []
  }

  @request_transitions %{
    requested: [:resolved, :rejected],
    resolved: [:authorized, :rejected],
    authorized: [:dispatched, :rejected],
    dispatched: [:completed, :failed],
    completed: [],
    failed: [],
    rejected: []
  }

  @workflow_transitions %{
    planned: [:queued, :cancelled],
    admitted: [:queued, :cancelled],
    queued: [:running, :cancelled, :expired],
    running: [:awaiting_review, :retry_scheduled, :degraded, :completed, :failed, :cancelled],
    awaiting_review: [:running, :rework_requested, :cancelled],
    retry_scheduled: [:running, :failed, :cancelled],
    degraded: [:running, :completed, :failed, :cancelled],
    rework_requested: [:queued, :cancelled],
    completed: [:archived],
    failed: [:archived],
    cancelled: [:archived],
    expired: [:archived],
    archived: []
  }

  @spec transition(struct(), atom() | tuple()) :: {:ok, struct()} | {:error, term()}
  def transition(%SourceItem{} = source_item, next_state) when is_atom(next_state),
    do: transition_state(source_item, next_state, @source_transitions)

  def transition(%OperationRequest{} = request, next_state) when is_atom(next_state),
    do: transition_state(request, next_state, @request_transitions)

  def transition(%WorkflowRun{} = workflow, next_state) when is_atom(next_state),
    do: transition_state(workflow, next_state, @workflow_transitions)

  def transition(%ReviewCase{} = review, {:record_decision, decision_ref})
      when is_binary(decision_ref) do
    decisions = Enum.uniq(review.decisions ++ [decision_ref])

    state =
      if length(decisions) >= review.required_decisions do
        :decided
      else
        :pending
      end

    {:ok, %{review | decisions: decisions, state: state}}
  end

  def transition(%ReviewCase{} = review, :cancel), do: {:ok, %{review | state: :cancelled}}

  def transition(_record, _transition), do: {:error, :unsupported_transition}

  defp transition_state(record, next_state, transition_map) do
    allowed_states = Map.get(transition_map, record.state, [])

    if next_state in allowed_states do
      {:ok, %{record | state: next_state}}
    else
      {:error, {:invalid_transition, record.state, next_state}}
    end
  end
end
