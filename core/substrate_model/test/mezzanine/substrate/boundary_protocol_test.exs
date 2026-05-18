defmodule Mezzanine.Substrate.BoundaryProtocolTest do
  use ExUnit.Case, async: true

  alias GroundPlane.Boundary.Codec
  alias GroundPlane.Boundary.DispatchResult
  alias GroundPlane.Boundary.Envelope
  alias GroundPlane.Boundary.Fixtures
  alias GroundPlane.Boundary.Protocol

  defmodule EchoHandler do
    @behaviour Protocol

    @impl true
    def dispatch(%Envelope{} = envelope) do
      DispatchResult.new(%{
        status: "completed",
        response: %{
          envelope_digest: Envelope.digest(envelope),
          operation: envelope.operation,
          target: envelope.target
        },
        receipt_refs: ["receipt://tenant-a/#{envelope.target}"]
      })
    end
  end

  test "Mezzanine first-pass plane boundaries are serializable deterministic envelopes" do
    envelopes = Fixtures.boundary_envelopes()

    for key <- [
          :mezzanine_citadel,
          :mezzanine_jido,
          :mezzanine_execution_plane,
          :mezzanine_ai_trace
        ] do
      envelope = Map.fetch!(envelopes, key)

      assert envelope.origin == "mezzanine"
      assert envelope.metadata.transport == "direct-module"
      assert Codec.encode!(envelope) == Envelope.encode!(envelope)
      assert String.starts_with?(Envelope.digest(envelope), "sha256:")
    end
  end

  test "Mezzanine direct-module boundary dispatch uses the same serializable contract" do
    envelope = Fixtures.boundary_envelopes().mezzanine_citadel

    assert {:ok, result} = Protocol.dispatch(EchoHandler, envelope)
    assert result.status == "completed"
    assert result.response.target == "citadel"
    assert result.response.operation == "authority.authorize_operation"
    assert String.starts_with?(result.response.envelope_digest, "sha256:")
  end

  test "Mezzanine boundary envelopes reject local runtime values and raw credentials" do
    assert {:error, :boundary_reference_not_serializable} =
             Envelope.new(%{
               id: "boundary://mezzanine/jido/bad-runtime-value",
               origin: "mezzanine",
               target: "jido_integration",
               operation: "lower.invoke_operation",
               tenant_id: "tenant-a",
               payload: %{reference: make_ref()}
             })

    assert {:error, {:raw_credential_key_forbidden, "token"}} =
             Codec.encode(%{
               tenant_id: "tenant-a",
               payload: %{operation: "lower.invoke_operation"},
               token: "raw-secret"
             })
  end
end
