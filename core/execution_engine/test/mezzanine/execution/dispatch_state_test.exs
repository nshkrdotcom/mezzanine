defmodule Mezzanine.Execution.DispatchStateTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Execution.DispatchState

  test "reduced active targets retain legacy aliases for live-row drains" do
    assert DispatchState.active_targets() == [:queued, :in_flight, :accepted_active]

    assert DispatchState.canonical(:pending_dispatch) == :queued
    assert DispatchState.canonical(:dispatching) == :in_flight
    assert DispatchState.canonical(:dispatching_retry) == :in_flight
    assert DispatchState.canonical(:awaiting_receipt) == :accepted_active
    assert DispatchState.canonical(:running) == :accepted_active

    assert "queued" in DispatchState.active_state_strings()
    assert "pending_dispatch" in DispatchState.active_state_strings()
    assert "accepted_active" in DispatchState.accepted_active_state_strings()
    assert "awaiting_receipt" in DispatchState.accepted_active_state_strings()
  end
end
