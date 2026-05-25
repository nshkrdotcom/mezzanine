defmodule Mezzanine.Bridges.CitadelBridge.TransportTest do
  use ExUnit.Case, async: true

  alias Mezzanine.Bridges.CitadelBridge.Transport

  defmodule DirectTarget do
    def authorize(request, opts) do
      {:ok, %{"mode" => "direct", "request" => request, "timeout" => opts[:timeout]}}
    end
  end

  test "direct transport calls an explicitly supplied Citadel facade" do
    assert {:ok, result} =
             Transport.Direct.authorize(%{"tenant_ref" => "tenant://one"},
               target: DirectTarget,
               timeout: 50
             )

    assert result["mode"] == "direct"
    assert result["timeout"] == 50
  end

  test "distributed transport calls an explicitly supplied Citadel facade" do
    assert {:ok, result} =
             Transport.Distributed.authorize(%{"tenant_ref" => "tenant://one"},
               node: Node.self(),
               facade_module: DirectTarget,
               timeout: 1_000
             )

    assert result["mode"] == "direct"
  end

  test "fixture transport provides deterministic authority evidence" do
    assert {:ok, result} = Transport.Fixture.authorize(%{}, [])
    assert result["authority_ref"] == "authority://fixture/citadel"
  end

  test "runtime deps select a Citadel transport explicitly" do
    assert {:ok, deps} =
             Transport.RuntimeDeps.new(
               transport: Transport.Fixture,
               transport_opts: [authorize: {:ok, %{"status" => "denied"}}]
             )

    assert {:ok, %{"status" => "denied"}} = Transport.RuntimeDeps.authorize(deps, %{})
  end
end
