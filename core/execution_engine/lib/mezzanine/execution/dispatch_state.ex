defmodule Mezzanine.Execution.DispatchState do
  @moduledoc """
  Canonical Phase 5 execution dispatch-state vocabulary.

  Active execution dispatch state is canonical-only after the strict Temporal
  cutover drain gate.
  """

  @active_targets [:queued, :in_flight, :accepted_active]
  @terminal_states [:completed, :cancelled, :failed, :rejected, :stalled]
  @all_states @active_targets ++ @terminal_states
  @accepted_active_states [:accepted_active]
  @in_flight_states [:in_flight]
  @canonical_by_atom Map.new(@all_states, &{&1, &1})
  @canonical_by_string Map.new(@canonical_by_atom, fn {state, canonical} ->
                         {Atom.to_string(state), canonical}
                       end)

  @spec active_targets() :: [atom()]
  def active_targets, do: @active_targets

  @spec active_states() :: [atom()]
  def active_states, do: @active_targets

  @spec active_state_strings() :: [String.t()]
  def active_state_strings, do: strings(active_states())

  @spec all_states() :: [atom()]
  def all_states, do: @all_states

  @spec accepted_active_state_strings() :: [String.t()]
  def accepted_active_state_strings, do: strings(@accepted_active_states)

  @spec in_flight_state_strings() :: [String.t()]
  def in_flight_state_strings, do: strings(@in_flight_states)

  @spec startup_reconcile_candidate_state_strings() :: [String.t()]
  def startup_reconcile_candidate_state_strings do
    in_flight_state_strings() ++ accepted_active_state_strings()
  end

  @spec canonical(atom() | String.t()) :: atom() | String.t()
  def canonical(state) when is_atom(state), do: Map.get(@canonical_by_atom, state, state)
  def canonical(state) when is_binary(state), do: Map.get(@canonical_by_string, state, state)

  defp strings(states), do: Enum.map(states, &Atom.to_string/1)
end
