defmodule Mezzanine.RemoteFacade.WorkflowTest do
  use ExUnit.Case, async: true

  alias Mezzanine.RemoteFacade.Workflow

  test "declares owner-defined workflow group" do
    assert Workflow.owner_group() == {Workflow, :workflow}
  end

  test "accepts governed work and returns accepted-ref readback contract" do
    assert {:ok, result} = Workflow.submit_work(valid_envelope())

    assert result["status"] == "accepted"
    assert result["accepted_ref"] == "mezzanine-work://idem%3A%2F%2Fone"
    assert result["readback_ref"] == result["accepted_ref"] <> "/readback"
    assert result["async_contract"] == "accepted_ref_plus_readback"
  end

  test "rejects missing authority" do
    assert {:error, %{"code" => "invalid_envelope", "missing_field" => "authority_ref"}} =
             valid_envelope()
             |> Map.delete("authority_ref")
             |> Workflow.submit_work()
  end

  test "rejects raw payloads" do
    assert {:error, %{"code" => "payload_not_allowed"}} =
             valid_envelope()
             |> Map.put("payload_mode", "raw_payload")
             |> Workflow.submit_work()
  end

  test "readback returns bounded projection facts" do
    assert {:ok, result} = Workflow.readback("mezzanine-work://one")

    assert result["accepted_ref"] == "mezzanine-work://one"
    assert result["projection_ref"] == "mezzanine-work://one/projection"
    assert result["owner"] == "mezzanine"
  end

  defp valid_envelope do
    %{
      "schema_ref" => "mezzanine.workflow.submit.v1",
      "tenant_ref" => "tenant://one",
      "correlation_ref" => "corr://one",
      "idempotency_key" => "idem://one",
      "trace_ref" => "trace://one",
      "authority_ref" => "authority://one",
      "payload_mode" => "refs_only",
      "redaction_class" => "tenant_sensitive"
    }
  end
end
