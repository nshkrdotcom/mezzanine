defmodule Mezzanine.AIRun.PersistencePostureTest do
  use ExUnit.Case, async: true

  alias Mezzanine.AIRun.{Envelope, PersistenceRefs}

  test "requires an explicit durable AI run persistence posture before retained writes" do
    assert {:error, :persistence_profile_required} = PersistenceRefs.resolve(nil)

    assert {:ok, profile} = PersistenceRefs.production_profile(%{})
    assert profile.id == :ops_durable
    assert profile.store_category == :ai_run_envelope
    assert profile.selected_tier == :postgres_shared
    assert profile.capture_level == :standard_redacted
    assert profile.restart_safe?
    assert profile.durable?

    assert {:ok, envelope} =
             Envelope.new(%{
               ai_run_ref: "ai_run://persist/default",
               run_class: :closed_loop_adaptation,
               tenant_ref: "tenant://demo",
               authority_ref: "authority://decision/1",
               actor_ref: "actor://operator/1",
               persistence_profile_ref: profile
             })

    assert envelope.persistence_profile_ref.selected_tier == :postgres_shared
  end

  test "memory and omitted profile identifiers fail closed" do
    assert {:error, {:durable_profile_unavailable, :mickey_mouse}} =
             PersistenceRefs.resolve(%{id: :mickey_mouse, selected_tier: :memory_ephemeral})

    assert {:error, {:durable_profile_unavailable, nil}} = PersistenceRefs.resolve(%{})
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
