defmodule Mezzanine.WorkflowRuntime.Application do
  @moduledoc false

  use Application

  alias Mezzanine.WorkflowRuntime.TemporalSupervisor

  @doc "Production children selected explicitly by the NSHKR composition root."
  def production_child_specs(opts) do
    temporal = Keyword.fetch!(opts, :temporal)
    dispatcher = Keyword.fetch!(opts, :outbox_dispatcher)

    TemporalSupervisor.child_specs(temporal) ++
      [{Mezzanine.WorkflowRuntime.RunOutboxDispatcher, dispatcher}]
  end

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(
      TemporalSupervisor.child_specs(),
      strategy: :one_for_one,
      name: Mezzanine.WorkflowRuntime.Supervisor
    )
  end
end
