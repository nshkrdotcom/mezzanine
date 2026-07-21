defmodule Mezzanine.WorkflowRuntime.Store do
  @moduledoc "Durable persistence facade for canonical Mezzanine run state."

  alias Mezzanine.WorkflowRuntime.Store.Postgres

  @callback capabilities() :: Mezzanine.Persistence.store_capability()
  @callback preflight(keyword()) :: :ok | {:error, term()}
  @callback accept_run(Mezzanine.Runs.AcceptCommand.t(), keyword()) ::
              {:ok, Mezzanine.Runs.Acceptance.t()} | {:error, term()}
  @callback fetch_acceptance(String.t(), keyword()) ::
              {:ok, Mezzanine.Runs.Acceptance.t()} | {:error, term()}
  @callback fetch_projection(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback list_events(String.t(), Mezzanine.Runs.EventCursor.t() | nil, keyword()) ::
              {:ok, [Mezzanine.Runs.Event.t()]} | {:error, term()}
  @callback read_cursor(String.t(), keyword()) ::
              {:ok, Mezzanine.Runs.EventCursor.t()} | {:error, term()}
  @callback claim_workflow_handoffs(String.t(), pos_integer(), keyword()) ::
              {:ok, [Mezzanine.Runs.WorkflowHandoff.t()]} | {:error, term()}
  @callback complete_workflow_handoff(String.t(), String.t(), String.t() | nil, keyword()) ::
              {:ok, Mezzanine.Runs.WorkflowHandoff.t()} | {:error, term()}
  @callback start_model_turn(Mezzanine.WorkflowRuntime.ModelTurnStart.t(), keyword()) ::
              {:ok, map()} | {:error, term()}
  @callback append_provider_event(Mezzanine.WorkflowRuntime.ProviderEvent.t(), keyword()) ::
              {:ok, Mezzanine.WorkflowRuntime.ProviderEvent.t()} | {:error, term()}
  @callback commit_provider_event(String.t(), keyword()) ::
              {:ok, Mezzanine.WorkflowRuntime.ProviderEvent.t()} | {:error, term()}
  @callback complete_model_turn(Mezzanine.WorkflowRuntime.ModelTurnCompletion.t(), keyword()) ::
              {:ok, map()} | {:error, term()}
  @callback fetch_model_turn(String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  @callback list_provider_events(String.t(), non_neg_integer(), keyword()) ::
              {:ok, [Mezzanine.WorkflowRuntime.ProviderEvent.t()]} | {:error, term()}
  @callback read_model_turn_cursor(String.t(), keyword()) ::
              {:ok, Mezzanine.WorkflowRuntime.ModelTurnCursor.t()} | {:error, term()}
  @callback health(keyword()) :: {:ok, map()} | {:error, term()}

  def adapter do
    case Application.fetch_env!(:mezzanine_core, :run_store) do
      Postgres -> Postgres
      forbidden -> raise "non-production Mezzanine run store configured: #{inspect(forbidden)}"
    end
  end

  def capabilities, do: adapter().capabilities()
  def preflight(opts \\ []), do: adapter().preflight(opts)
  def accept_run(command, opts \\ []), do: adapter().accept_run(command, opts)
  def fetch_acceptance(command_ref, opts \\ []), do: adapter().fetch_acceptance(command_ref, opts)
  def fetch_projection(run_ref, opts \\ []), do: adapter().fetch_projection(run_ref, opts)

  def list_events(run_ref, cursor \\ nil, opts \\ []),
    do: adapter().list_events(run_ref, cursor, opts)

  def read_cursor(run_ref, opts \\ []), do: adapter().read_cursor(run_ref, opts)

  def claim_workflow_handoffs(lock_owner, limit, opts \\ []),
    do: adapter().claim_workflow_handoffs(lock_owner, limit, opts)

  def complete_workflow_handoff(outbox_ref, state, error_ref \\ nil, opts \\ []),
    do: adapter().complete_workflow_handoff(outbox_ref, state, error_ref, opts)

  def start_model_turn(start, opts \\ []), do: adapter().start_model_turn(start, opts)
  def append_provider_event(event, opts \\ []), do: adapter().append_provider_event(event, opts)

  def commit_provider_event(event_ref, opts \\ []),
    do: adapter().commit_provider_event(event_ref, opts)

  def complete_model_turn(completion, opts \\ []),
    do: adapter().complete_model_turn(completion, opts)

  def fetch_model_turn(turn_ref, opts \\ []), do: adapter().fetch_model_turn(turn_ref, opts)

  def list_provider_events(turn_ref, after_sequence \\ 0, opts \\ []),
    do: adapter().list_provider_events(turn_ref, after_sequence, opts)

  def read_model_turn_cursor(turn_ref, opts \\ []),
    do: adapter().read_model_turn_cursor(turn_ref, opts)

  def health(opts \\ []), do: adapter().health(opts)
end
