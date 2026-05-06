defmodule Mezzanine.AIRun.PersistencePostureTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AIRun.{Envelope, PersistenceRefs}

  test "resolves default AI run persistence posture before retained writes" do
    assert profile = PersistenceRefs.default_profile()
    assert profile.id == :mickey_mouse
    assert profile.store_category == :ai_run_envelope
    assert profile.selected_tier == :memory_ephemeral
    assert profile.capture_level == :minimal_refs
    refute profile.restart_safe?

    assert {:ok, envelope} =
             Envelope.new(%{
               ai_run_ref: "ai_run://persist/default",
               run_class: :closed_loop_adaptation,
               tenant_ref: "tenant://demo",
               authority_ref: "authority://decision/1",
               actor_ref: "actor://operator/1",
               persistence_profile_ref: profile
             })

    assert envelope.persistence_profile_ref.selected_tier == :memory_ephemeral
  end

  test "explicit unavailable durable profile fails closed" do
    assert {:error, {:durable_profile_unavailable, :ops_durable}} =
             PersistenceRefs.resolve(%{
               id: :ops_durable,
               selected_tier: :temporal_postgres,
               store_category: :ai_run_envelope,
               capture_level: :standard_redacted,
               available?: false
             })
  end
end
