defmodule Mezzanine.Bridges.OuterBrainBridge.TransportTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Bridges.OuterBrainBridge.Transport

  defmodule DirectTarget do
    def compile_context(request, opts) do
      {:ok, %{"mode" => "direct", "request" => request, "timeout" => opts[:timeout]}}
    end

    def readback_context(ref), do: {:ok, %{"mode" => "direct", "ref" => ref}}
  end

  test "direct transport calls an explicitly supplied OuterBrain facade" do
    assert {:ok, result} =
             Transport.Direct.compile_context(%{"tenant_ref" => "tenant://one"},
               target: DirectTarget,
               timeout: 50
             )

    assert result["mode"] == "direct"
    assert result["timeout"] == 50
  end

  test "distributed transport calls an explicitly supplied OuterBrain facade" do
    assert {:ok, result} =
             Transport.Distributed.compile_context(%{"tenant_ref" => "tenant://one"},
               node: Node.self(),
               facade_module: DirectTarget,
               timeout: 1_000
             )

    assert result["mode"] == "direct"
  end

  test "fixture transport provides deterministic context evidence" do
    assert {:ok, result} = Transport.Fixture.compile_context(%{}, [])
    assert result["context_packet_ref"] == "context://fixture/packet"
  end

  test "runtime deps select an OuterBrain transport explicitly" do
    assert {:ok, deps} =
             Transport.RuntimeDeps.new(
               transport: Transport.Fixture,
               transport_opts: [compile_context: {:ok, %{"packet_hash" => "sha256:fixed"}}]
             )

    assert {:ok, %{"packet_hash" => "sha256:fixed"}} =
             Transport.RuntimeDeps.compile_context(deps, %{})
  end
end
