defmodule Mezzanine.Substrate.HealthTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Mezzanine.Substrate.Health
  alias Mix.Tasks.Mezzanine.Substrate.Health, as: HealthTask

  test "reports the required provider-free substrate checks" do
    report = Health.report()

    assert :ok = Health.validate_report(report)
    assert report.schema_version == "mezzanine_substrate_health_v1"
    assert report.profile == "deployment_single_node"
    assert report.proof_posture.production_deployment_proven? == false
    assert report.proof_posture.authoritative_audit? == false

    check_names = Enum.map(report.checks, & &1.name)
    assert check_names == ~w(postgres temporal outer_brain_provider_free websocket_edge)

    temporal = Enum.find(report.checks, &(&1.name == "temporal"))
    assert temporal.attributes["status_command"] == "just dev-status"
    assert temporal.attributes["expected_endpoint"] == "127.0.0.1:7233"
    refute report.temporal_guardrail.raw_temporal_server_start_allowed?
    refute report.temporal_guardrail.destructive_reset_allowed_without_operator_approval?
    assert "just dev-status" in report.temporal_guardrail.allowed_commands
  end

  test "rejects unsafe production proof posture" do
    report =
      Health.report()
      |> put_in([:proof_posture, :production_deployment_proven?], true)

    assert {:error, failures} = Health.validate_report(report)
    assert Enum.any?(failures, &(&1.code == "health_bad_posture"))
  end

  test "mix task prints the substrate health surface" do
    output =
      capture_io(fn ->
        HealthTask.run([])
      end)

    assert output =~ "schema_version=mezzanine_substrate_health_v1"
    assert output =~ "postgres: configured_for_rehearsal"
    assert output =~ "temporal: owned_local_substrate"
    assert output =~ "outer_brain_provider_free: declared_provider_free_boundary"
    assert output =~ "websocket_edge: reconnect_readback_required"
    assert output =~ "raw_temporal_server_start_allowed?=false"
  end
end
