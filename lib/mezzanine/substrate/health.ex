defmodule Mezzanine.Substrate.Health do
  @moduledoc """
  Provider-free substrate health report for local deployment rehearsals.

  This report is intentionally declarative and non-destructive. It names the
  Mezzanine-owned commands and boundaries that an operator should use for a
  local single-node rehearsal without starting external services from code.
  """

  @schema_version "mezzanine_substrate_health_v1"
  @profile "deployment_single_node"
  @temporal_commands [
    "just dev-up",
    "just dev-status",
    "just dev-logs",
    "just temporal-ui",
    "just dev-down"
  ]
  @required_checks ~w(postgres temporal outer_brain_provider_free websocket_edge)

  @type check :: %{
          name: String.t(),
          status: String.t(),
          owner_repo: String.t(),
          evidence_ref: String.t(),
          summary: String.t(),
          attributes: map()
        }

  @type report :: %{
          schema_version: String.t(),
          profile: String.t(),
          command: String.t(),
          checks: [check()],
          proof_posture: map(),
          temporal_guardrail: map()
        }

  @spec report(keyword()) :: report()
  def report(opts \\ []) do
    %{
      schema_version: @schema_version,
      profile: @profile,
      command: Keyword.get(opts, :command, "mix mezzanine.substrate.health"),
      checks: checks(),
      proof_posture: proof_posture(),
      temporal_guardrail: temporal_guardrail()
    }
  end

  @spec validate_report(report()) :: :ok | {:error, [map()]}
  def validate_report(%{} = report) do
    failures =
      []
      |> require_equal("health_bad_schema", report.schema_version, @schema_version)
      |> require_equal("health_bad_profile", report.profile, @profile)
      |> validate_required_checks(report.checks)
      |> validate_posture(report.proof_posture)
      |> validate_temporal_guardrail(report.temporal_guardrail)

    case failures do
      [] -> :ok
      failures -> {:error, Enum.reverse(failures)}
    end
  end

  def validate_report(_report), do: {:error, [failure("health_invalid_report")]}

  @spec format(report()) :: String.t()
  def format(report) do
    check_lines =
      Enum.map_join(report.checks, "\n", fn check ->
        "- #{check.name}: #{check.status} (owner=#{check.owner_repo}, evidence=#{check.evidence_ref})"
      end)

    """
    schema_version=#{report.schema_version}
    profile=#{report.profile}
    command=#{report.command}
    production_deployment_proven?=#{report.proof_posture.production_deployment_proven?}
    authoritative_audit?=#{report.proof_posture.authoritative_audit?}
    raw_temporal_server_start_allowed?=#{report.temporal_guardrail.raw_temporal_server_start_allowed?}
    temporal_expected_endpoint=#{report.temporal_guardrail.expected_endpoint}
    temporal_expected_namespace=#{report.temporal_guardrail.expected_namespace}
    checks:
    #{check_lines}
    """
    |> String.trim_trailing()
  end

  defp checks do
    [
      check(
        "postgres",
        "configured_for_rehearsal",
        "mezzanine",
        "health://mezzanine/postgres/configured",
        "Postgres is the durable store expected by the local deployment rehearsal.",
        %{
          "destructive_restore_performed?" => false,
          "operator_receipt_required?" => true
        }
      ),
      check(
        "temporal",
        "owned_local_substrate",
        "mezzanine",
        "health://mezzanine/temporal/dev-status",
        "Temporal local substrate is controlled only through Mezzanine just commands.",
        %{
          "expected_endpoint" => "127.0.0.1:7233",
          "expected_ui" => "http://127.0.0.1:8233",
          "expected_namespace" => "default",
          "status_command" => "just dev-status"
        }
      ),
      check(
        "outer_brain_provider_free",
        "declared_provider_free_boundary",
        "outer_brain",
        "health://outer_brain/provider-free/not-started",
        "Deployment rehearsal must not require live model/provider calls.",
        %{
          "live_provider_required?" => false,
          "provider_payload_allowed?" => false
        }
      ),
      check(
        "websocket_edge",
        "reconnect_readback_required",
        "app_kit",
        "health://app_kit/websocket/reconnect-readback",
        "Product edge health is proven by reconnect and missed-event readback receipts.",
        %{
          "edge_owned_by" => "app_kit",
          "receipt_required?" => true
        }
      )
    ]
  end

  defp check(name, status, owner_repo, evidence_ref, summary, attributes) do
    %{
      name: name,
      status: status,
      owner_repo: owner_repo,
      evidence_ref: evidence_ref,
      summary: summary,
      attributes: attributes
    }
  end

  defp proof_posture do
    %{
      authoritative_audit?: false,
      production_deployment_proven?: false,
      safe_action: "use_as_local_single_node_rehearsal_health"
    }
  end

  defp temporal_guardrail do
    %{
      allowed_commands: @temporal_commands,
      expected_endpoint: "127.0.0.1:7233",
      expected_ui: "http://127.0.0.1:8233",
      expected_namespace: "default",
      owned_service: "mezzanine-temporal-dev.service",
      persistent_state: "~/.local/share/temporal/dev-server.db",
      raw_temporal_server_start_allowed?: false,
      destructive_reset_allowed_without_operator_approval?: false
    }
  end

  defp validate_required_checks(failures, checks) when is_list(checks) do
    present = checks |> Enum.map(& &1.name) |> MapSet.new()

    @required_checks
    |> Enum.reject(&MapSet.member?(present, &1))
    |> case do
      [] -> failures
      missing -> [failure("health_missing_required_check", checks: missing) | failures]
    end
  end

  defp validate_required_checks(failures, _checks) do
    [failure("health_missing_required_check", checks: @required_checks) | failures]
  end

  defp validate_posture(failures, %{} = posture) do
    if posture.authoritative_audit? == false and
         posture.production_deployment_proven? == false do
      failures
    else
      [failure("health_bad_posture", posture: posture) | failures]
    end
  end

  defp validate_posture(failures, posture) do
    [failure("health_bad_posture", posture: posture) | failures]
  end

  defp validate_temporal_guardrail(failures, %{} = guardrail) do
    if guardrail.raw_temporal_server_start_allowed? == false and
         guardrail.destructive_reset_allowed_without_operator_approval? == false and
         "just dev-status" in guardrail.allowed_commands do
      failures
    else
      [failure("health_bad_temporal_guardrail", guardrail: guardrail) | failures]
    end
  end

  defp validate_temporal_guardrail(failures, guardrail) do
    [failure("health_bad_temporal_guardrail", guardrail: guardrail) | failures]
  end

  defp require_equal(failures, _code, actual, expected) when actual == expected, do: failures

  defp require_equal(failures, code, actual, expected) do
    [failure(code, expected: expected, actual: actual) | failures]
  end

  defp failure(code, fields \\ []) do
    fields
    |> Map.new()
    |> Map.put(:code, code)
  end
end
