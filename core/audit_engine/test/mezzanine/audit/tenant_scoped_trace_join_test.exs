defmodule Mezzanine.Audit.TenantScopedTraceJoinTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Audit.TenantScopedTraceJoin

  test "accepts scoped trace joins with explicit included and excluded refs" do
    assert {:ok, join} = TenantScopedTraceJoin.new(valid_attrs())

    assert join.contract_name == "Platform.TenantScopedTraceJoin.v1"
    assert join.scope_proof_ref == "scope-proof://trace/075"

    assert Enum.map(join.joined_ref_set, & &1.source_ref) == [
             "audit://fact/1",
             "execution://record/1"
           ]

    assert Enum.map(join.excluded_ref_set, & &1.exclusion_reason) == ["tenant_mismatch"]
  end

  test "accepts JSON-decoded string keyed attrs" do
    assert {:ok, join} =
             valid_attrs()
             |> stringify_keys()
             |> TenantScopedTraceJoin.new()

    assert join.tenant_ref == "tenant:alpha"
    assert hd(join.joined_ref_set).staleness_class == :authoritative_hot
  end

  test "rejects joined refs from another tenant" do
    assert {:error, {:cross_tenant_join_ref, "lower://run/beta"}} =
             valid_attrs()
             |> put_in([:joined_ref_set], [
               %{
                 source_ref: "lower://run/beta",
                 source_family: "lower_fact",
                 tenant_ref: "tenant:beta",
                 resource_ref: "work://expense/1",
                 trace_id: "trace:075",
                 staleness_class: "lower_fresh"
               }
             ])
             |> TenantScopedTraceJoin.new()
  end

  test "rejects joined refs outside the declared resource scope" do
    assert {:error, {:resource_scope_violation, "audit://fact/out-of-scope"}} =
             valid_attrs()
             |> put_in([:joined_ref_set], [
               %{
                 source_ref: "audit://fact/out-of-scope",
                 source_family: "audit_fact",
                 tenant_ref: "tenant:alpha",
                 resource_ref: "work://other/2",
                 trace_id: "trace:075",
                 staleness_class: "authoritative_hot"
               }
             ])
             |> TenantScopedTraceJoin.new()
  end

  test "requires actor, authority, trace, scope proof, and joined refs" do
    assert {:error, {:missing_required_fields, fields}} =
             valid_attrs()
             |> Map.merge(%{
               principal_ref: nil,
               system_actor_ref: nil,
               authority_packet_ref: nil,
               trace_id: nil,
               scope_proof_ref: nil,
               joined_ref_set: []
             })
             |> TenantScopedTraceJoin.new()

    assert :principal_ref_or_system_actor_ref in fields
    assert :authority_packet_ref in fields
    assert :trace_id in fields
    assert :scope_proof_ref in fields
    assert :joined_ref_set in fields
  end

  defp valid_attrs do
    %{
      tenant_ref: "tenant:alpha",
      installation_ref: "installation:alpha-prod",
      workspace_ref: "workspace:ops",
      project_ref: "project:control-room",
      environment_ref: "environment:prod",
      principal_ref: "principal:operator/alice",
      resource_ref: "work://expense/1",
      authority_packet_ref: "authority-packet://trace/075",
      permission_decision_ref: "permission-decision://trace/075",
      idempotency_key: "tenant-trace-join:075",
      trace_id: "trace:075",
      correlation_id: "correlation:075",
      release_manifest_ref: "phase4-v6-milestone15",
      trace_join_ref: "trace-join://075",
      resource_scope: ["work://expense/1", "execution://expense/1"],
      joined_ref_set: [
        %{
          source_ref: "audit://fact/1",
          source_family: "audit_fact",
          tenant_ref: "tenant:alpha",
          resource_ref: "work://expense/1",
          trace_id: "trace:075",
          staleness_class: "authoritative_hot"
        },
        %{
          source_ref: "execution://record/1",
          source_family: "execution_record",
          tenant_ref: "tenant:alpha",
          resource_ref: "execution://expense/1",
          trace_id: "trace:075",
          staleness_class: "authoritative_hot"
        }
      ],
      excluded_ref_set: [
        %{
          source_ref: "lower://run/beta",
          source_family: "lower_fact",
          tenant_ref: "tenant:beta",
          resource_ref: "work://expense/1",
          trace_id: "trace:075",
          exclusion_reason: "tenant_mismatch"
        }
      ],
      scope_proof_ref: "scope-proof://trace/075"
    }
  end

  defp stringify_keys(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} -> {to_string(key), stringify_keys(nested_value)} end)
  end

  defp stringify_keys(value) when is_list(value), do: Enum.map(value, &stringify_keys/1)
  defp stringify_keys(value), do: value
end
