defmodule Mezzanine.Scheduler.TickLoopTest do
  use ExUnit.Case, async: false

  alias Mezzanine.Scheduler.TickLoop

  test "fires ticks at the configured interval and can be stopped cleanly" do
    test_pid = self()

    assert {:ok, pid} =
             TickLoop.start_link(
               interval_ms: 10,
               tick_fun: fn -> send(test_pid, :tick) end
             )

    assert_receive :tick, 100
    assert_receive :tick, 100

    assert :ok = GenServer.stop(pid)
    refute Process.alive?(pid)
  end
end
