defmodule Mezzanine.Scheduler.TickLoop do
  @moduledoc """
  Periodic scheduler tick owner.
  """

  use GenServer

  require Logger

  @default_interval_ms 5_000

  @type state :: %{
          interval_ms: pos_integer(),
          tick_fun: (-> term()),
          timer_ref: reference() | nil
        }

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)
    init_arg = Keyword.drop(opts, [:name])

    case name do
      nil -> GenServer.start_link(__MODULE__, init_arg)
      _name -> GenServer.start_link(__MODULE__, init_arg, name: name)
    end
  end

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @default_interval_ms),
      tick_fun: Keyword.get(opts, :tick_fun, fn -> :ok end),
      timer_ref: nil
    }

    {:ok, schedule_tick(state)}
  end

  @impl true
  def handle_info(:tick, %{tick_fun: tick_fun} = state) do
    _ = safe_tick(tick_fun)
    {:noreply, schedule_tick(%{state | timer_ref: nil})}
  end

  @impl true
  def terminate(_reason, %{timer_ref: timer_ref}) when is_reference(timer_ref) do
    Process.cancel_timer(timer_ref)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  defp schedule_tick(state) do
    %{state | timer_ref: Process.send_after(self(), :tick, state.interval_ms)}
  end

  defp safe_tick(tick_fun) do
    tick_fun.()
  rescue
    error ->
      Logger.error(Exception.format(:error, error, __STACKTRACE__))
      {:error, error}
  catch
    kind, value ->
      Logger.error(Exception.format(kind, value, __STACKTRACE__))
      {:error, {kind, value}}
  end
end
