defmodule Mezzanine.Lifecycle.DispatchEnvelopeRefValidatorTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Lifecycle.DispatchEnvelopeRefValidator

  test "accepts Phase 2 opaque refs without runtime-auth semantics" do
    assert :ok = DispatchEnvelopeRefValidator.validate(valid_input())
  end

  test "reports canonical missing ref errors" do
    assert {:error, :missing_trace_id} =
             valid_input(trace_id: nil)
             |> DispatchEnvelopeRefValidator.validate()

    assert {:error, :missing_idempotency_key} =
             valid_input(submission_dedupe_key: nil)
             |> DispatchEnvelopeRefValidator.validate()

    assert {:error, :missing_installation_revision} =
             valid_input(compiled_pack_revision: nil)
             |> DispatchEnvelopeRefValidator.validate()

    assert {:error, :missing_connector_binding_ref} =
             valid_input(
               binding_snapshot: Map.delete(valid_binding_snapshot(), "connector_binding_ref")
             )
             |> DispatchEnvelopeRefValidator.validate()
  end

  test "reports canonical credential posture and safety errors" do
    assert {:error, :missing_credential_lease_ref} =
             valid_input(
               dispatch_envelope:
                 valid_dispatch_envelope()
                 |> Map.delete("no_credentials_posture_ref")
                 |> Map.put("credentials_required", true)
             )
             |> DispatchEnvelopeRefValidator.validate()

    assert {:error, :raw_credential_material_forbidden} =
             valid_input(
               binding_snapshot: Map.put(valid_binding_snapshot(), "client_secret", "raw")
             )
             |> DispatchEnvelopeRefValidator.validate()
  end

  test "reports stale revision and tenant mismatch when locally derivable" do
    assert {:error, :stale_installation_revision} =
             valid_input(
               binding_snapshot: Map.put(valid_binding_snapshot(), "installation_revision", 7)
             )
             |> DispatchEnvelopeRefValidator.validate()

    assert {:error, :tenant_installation_mismatch} =
             valid_input(
               binding_snapshot: Map.put(valid_binding_snapshot(), "tenant_id", "tenant-other")
             )
             |> DispatchEnvelopeRefValidator.validate()
  end

  defp valid_input(overrides \\ []) do
    Map.merge(
      %{
        tenant_id: "tenant-1",
        installation_id: "installation-1",
        subject_id: "subject-1",
        compiled_pack_revision: 1,
        binding_snapshot: valid_binding_snapshot(),
        dispatch_envelope: valid_dispatch_envelope(),
        intent_snapshot: %{},
        submission_dedupe_key: "dedupe-1",
        trace_id: "trace-1"
      },
      Map.new(overrides)
    )
  end

  defp valid_binding_snapshot do
    %{
      "authority_decision_ref" => "authority-decision://fixture/expense_capture",
      "connector_binding_ref" => "connector-binding://expense_system_api",
      "no_credentials_posture_ref" => "no-credentials://fixture/expense_capture"
    }
  end

  defp valid_dispatch_envelope do
    %{
      "recipe_ref" => "expense_capture",
      "authority_decision_ref" => "authority-decision://fixture/expense_capture",
      "no_credentials_posture_ref" => "no-credentials://fixture/expense_capture",
      "dispatch_ref_requirements" => %{
        "authority_decision_ref" => "required",
        "connector_binding_ref" => "required",
        "credential_posture_ref" => "credential_lease_or_no_credentials"
      }
    }
  end
end
