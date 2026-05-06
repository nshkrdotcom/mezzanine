defmodule Mezzanine.AIRun.MemoryDefaultBootTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AIRun.PersistenceRefs

  test "memory default boot touches no durable or debug substrate" do
    assert boot = PersistenceRefs.memory_default_boot()
    assert boot.profile_id == :mickey_mouse
    assert boot.store_tiers == [:memory_ephemeral]
    assert boot.disabled_substrates == [:postgres, :temporal, :object_store, :debug_sidecar]
    refute boot.restart_safe?
    refute boot.debug_capture?
  end
end
