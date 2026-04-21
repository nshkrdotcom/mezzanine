defmodule Mezzanine.Execution.DispatchStateTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Execution.DispatchState

  test "strict cutover keeps only canonical active dispatch states" do
    assert DispatchState.active_targets() == [:queued, :in_flight, :accepted_active]
    assert DispatchState.active_states() == DispatchState.active_targets()

    assert DispatchState.canonical(:queued) == :queued
    assert DispatchState.canonical(:in_flight) == :in_flight
    assert DispatchState.canonical(:accepted_active) == :accepted_active
    assert DispatchState.canonical(:pending_dispatch) == :pending_dispatch
    assert DispatchState.canonical(:dispatching) == :dispatching
    assert DispatchState.canonical(:dispatching_retry) == :dispatching_retry
    assert DispatchState.canonical(:awaiting_receipt) == :awaiting_receipt
    assert DispatchState.canonical(:running) == :running

    assert DispatchState.active_state_strings() == ["queued", "in_flight", "accepted_active"]
    assert DispatchState.accepted_active_state_strings() == ["accepted_active"]
    assert DispatchState.in_flight_state_strings() == ["in_flight"]

    assert DispatchState.startup_reconcile_candidate_state_strings() == [
             "in_flight",
             "accepted_active"
           ]
  end
end
